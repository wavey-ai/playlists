//! The send path of the cache mesh: RaptorQ encoding with a cached plan for
//! each source-symbol count, and the datagrams of a pass handed to the kernel
//! in batches (`sendmmsg`, with UDP generic segmentation offload where the
//! kernel has it).
//!
//! The datagrams are byte for byte those of
//! [`raptorq_datagram_fec::DatagramFecEncoder`], so a receiver cannot tell
//! the two apart.

use bytes::{BufMut, Bytes, BytesMut};
use raptorq::{ObjectTransmissionInformation, SourceBlockEncoder, SourceBlockEncodingPlan};
use raptorq_datagram_fec::{
    source_symbol_count, DatagramFecError, DatagramFecHeader, ENCODING_PACKET_HEADER_LEN,
    HEADER_LEN, MAX_SOURCE_SYMBOLS_PER_BLOCK,
};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

/// Encoding plans kept, by source-symbol count. A plan is what makes an
/// encoding cheap: without it every object solves the RaptorQ constraint
/// matrix again.
const PLAN_CACHE: usize = 128;

/// Messages handed to the kernel in one `sendmmsg` call.
pub(crate) const MESSAGES_PER_CALL: usize = 64;

/// Largest datagram that is segmented by the kernel (UDP GSO): one that fits
/// a 1500-byte IPv6 packet. A larger one is sent on its own.
pub(crate) const GSO_MAX_DATAGRAM: usize = 1_452;

/// Most segments in one GSO message (the kernel's limit is 64 on older
/// kernels), and at most this many bytes.
const GSO_MAX_SEGMENTS: usize = 64;
const GSO_MAX_BYTES: usize = 65_000;

/// The datagrams of one object, back to back in one buffer: first the source
/// symbols, then the repair symbols. Every datagram has the same length.
#[derive(Clone, Debug)]
pub(crate) struct ObjectDatagrams {
    pub wire: Bytes,
    pub datagram_len: usize,
    pub source_symbols: usize,
}

impl ObjectDatagrams {
    pub fn count(&self) -> usize {
        self.wire.len() / self.datagram_len.max(1)
    }

    pub fn datagram(&self, index: usize) -> Bytes {
        self.wire
            .slice(index * self.datagram_len..(index + 1) * self.datagram_len)
    }
}

/// A RaptorQ encoder that numbers blocks and packets as
/// [`raptorq_datagram_fec::DatagramFecEncoder`] does and keeps the encoding
/// plan of each source-symbol count.
pub(crate) struct ObjectEncoder {
    block_id: u32,
    packet_sequence: u32,
    plans: HashMap<u16, Arc<SourceBlockEncodingPlan>>,
    plan_order: VecDeque<u16>,
    padded: Vec<u8>,
    /// The repair symbols of a block of one source symbol, by repair index.
    single: Vec<SingleSymbolRepair>,
}

/// A repair symbol of a block of one source symbol, as a map of bytes.
///
/// RaptorQ treats each byte position of the symbols on its own: a byte of a
/// repair symbol depends only on the bytes at the same position of the
/// source symbols. With one source symbol it is a fixed function of one
/// byte, read here from the encoding of the symbol 0, 1, .., 255. A part
/// that fits one symbol (any audio part) is then repaired by a table lookup
/// per byte instead of a RaptorQ encoding.
struct SingleSymbolRepair {
    payload_id: [u8; 4],
    table: [u8; 256],
}

impl ObjectEncoder {
    pub fn new(initial_block_id: u32) -> Self {
        Self {
            block_id: initial_block_id,
            packet_sequence: 0,
            plans: HashMap::new(),
            plan_order: VecDeque::new(),
            padded: Vec::new(),
            single: Vec::new(),
        }
    }

    /// The maps of the first `count` repair symbols of a one-symbol block.
    fn single_symbol_repairs(&mut self, count: usize) {
        if self.single.len() >= count {
            return;
        }
        let identity = (0..=255_u8).collect::<Vec<_>>();
        let plan = self.plan(1);
        let config = ObjectTransmissionInformation::new(256, 256, 1, 1, 1);
        let encoder = SourceBlockEncoder::with_encoding_plan(0, &config, &identity, &plan);
        self.single = encoder
            .repair_packets(0, count as u32)
            .into_iter()
            .map(|packet| {
                let mut table = [0_u8; 256];
                table.copy_from_slice(packet.data());
                SingleSymbolRepair {
                    payload_id: packet.payload_id().serialize(),
                    table,
                }
            })
            .collect();
    }

    #[cfg(test)]
    pub fn plans(&self) -> usize {
        self.plans.len()
    }

    fn plan(&mut self, source_symbols: u16) -> Arc<SourceBlockEncodingPlan> {
        if let Some(plan) = self.plans.get(&source_symbols) {
            return Arc::clone(plan);
        }
        let plan = Arc::new(SourceBlockEncodingPlan::generate(source_symbols));
        if self.plans.len() >= PLAN_CACHE {
            if let Some(old) = self.plan_order.pop_front() {
                self.plans.remove(&old);
            }
        }
        self.plans.insert(source_symbols, Arc::clone(&plan));
        self.plan_order.push_back(source_symbols);
        plan
    }

    /// Encode the object that is the concatenation of `pieces` as one block
    /// of `symbol_size` symbols with `repair_symbols` repair symbols.
    pub fn encode(
        &mut self,
        pieces: &[&[u8]],
        symbol_size: u16,
        repair_symbols: u32,
    ) -> Result<ObjectDatagrams, DatagramFecError> {
        let len = pieces.iter().map(|piece| piece.len()).sum::<usize>();
        if len > u32::MAX as usize {
            return Err(DatagramFecError::PayloadTooLong { actual: len });
        }
        let symbol_size = symbol_size.max(1);
        let size = usize::from(symbol_size);
        let source_symbols = source_symbol_count(len, symbol_size);
        if u32::from(source_symbols) > MAX_SOURCE_SYMBOLS_PER_BLOCK
            || len > usize::from(source_symbols) * size
        {
            return Err(DatagramFecError::PayloadTooLargeForBlock {
                actual: len,
                max: MAX_SOURCE_SYMBOLS_PER_BLOCK as usize * size,
            });
        }
        let source = usize::from(source_symbols);

        let mut padded = std::mem::take(&mut self.padded);
        padded.clear();
        for piece in pieces {
            padded.extend_from_slice(piece);
        }
        padded.resize(source * size, 0);

        let datagram_len = HEADER_LEN + ENCODING_PACKET_HEADER_LEN + size;
        let total = source + repair_symbols as usize;
        let mut wire = BytesMut::with_capacity(total * datagram_len);
        let result = (|| {
            for (index, symbol) in padded.chunks(size).enumerate() {
                let id = [0, (index >> 16) as u8, (index >> 8) as u8, index as u8];
                self.put_datagram(
                    &mut wire,
                    len as u32,
                    source_symbols,
                    symbol_size,
                    id,
                    symbol,
                )?;
            }
            if repair_symbols > 0 && source == 1 {
                self.single_symbol_repairs(repair_symbols as usize);
                for index in 0..repair_symbols as usize {
                    let start = wire.len();
                    wire.put_bytes(0, HEADER_LEN);
                    wire.put_slice(&self.single[index].payload_id);
                    let table = &self.single[index].table;
                    wire.extend(padded.iter().map(|byte| table[usize::from(*byte)]));
                    self.seal_datagram(&mut wire, start, len as u32, source_symbols, symbol_size)?;
                }
            } else if repair_symbols > 0 {
                let plan = self.plan(source_symbols);
                let config = ObjectTransmissionInformation::new(len as u64, symbol_size, 1, 1, 1);
                let encoder = SourceBlockEncoder::with_encoding_plan(0, &config, &padded, &plan);
                for packet in encoder.repair_packets(0, repair_symbols) {
                    self.put_datagram(
                        &mut wire,
                        len as u32,
                        source_symbols,
                        symbol_size,
                        packet.payload_id().serialize(),
                        packet.data(),
                    )?;
                }
            }
            Ok(())
        })();
        self.padded = padded;
        result?;
        self.block_id = self.block_id.wrapping_add(1);
        Ok(ObjectDatagrams {
            wire: wire.freeze(),
            datagram_len,
            source_symbols: source,
        })
    }

    fn put_datagram(
        &mut self,
        wire: &mut BytesMut,
        transfer_length: u32,
        source_symbols: u16,
        symbol_size: u16,
        payload_id: [u8; 4],
        symbol: &[u8],
    ) -> Result<(), DatagramFecError> {
        let start = wire.len();
        wire.put_bytes(0, HEADER_LEN);
        wire.put_slice(&payload_id);
        wire.put_slice(symbol);
        self.seal_datagram(wire, start, transfer_length, source_symbols, symbol_size)
    }

    /// Write the header of the datagram that starts at `start` and runs to
    /// the end of `wire`.
    fn seal_datagram(
        &mut self,
        wire: &mut BytesMut,
        start: usize,
        transfer_length: u32,
        source_symbols: u16,
        symbol_size: u16,
    ) -> Result<(), DatagramFecError> {
        let header = DatagramFecHeader::raptorq(
            self.block_id,
            transfer_length,
            self.packet_sequence,
            source_symbols,
            symbol_size,
            &wire[start + HEADER_LEN..],
        )?;
        header.encode(&mut wire[start..start + HEADER_LEN])?;
        self.packet_sequence = self.packet_sequence.wrapping_add(1);
        Ok(())
    }
}

/// What one send call hands to the kernel: one datagram, or (`segment` > 0)
/// datagrams of `segment` bytes back to back, which the kernel splits.
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub bytes: Bytes,
    pub segment: usize,
}

impl Message {
    pub fn datagrams(&self) -> usize {
        if self.segment == 0 {
            1
        } else {
            self.bytes.len().div_ceil(self.segment)
        }
    }
}

/// Hands the datagrams of a pass to the kernel, a batch at a time.
pub(crate) struct BatchSender {
    gso: bool,
}

impl BatchSender {
    /// `gso` asks for generic segmentation offload; it is used only when the
    /// kernel has it.
    pub fn new(socket: &UdpSocket, gso: bool) -> Self {
        Self {
            gso: gso && gso_supported(socket),
        }
    }

    pub fn gso(&self) -> bool {
        self.gso
    }

    /// Queue the first `count` datagrams of `object`.
    pub fn push(&self, messages: &mut Vec<Message>, object: &ObjectDatagrams, count: usize) {
        let count = count.min(object.count());
        let len = object.datagram_len;
        if self.gso && count > 1 && len <= GSO_MAX_DATAGRAM {
            let per_message = (GSO_MAX_BYTES / len).clamp(1, GSO_MAX_SEGMENTS);
            let mut start = 0;
            while start < count {
                let end = (start + per_message).min(count);
                messages.push(Message {
                    bytes: object.wire.slice(start * len..end * len),
                    segment: if end - start > 1 { len } else { 0 },
                });
                start = end;
            }
        } else {
            messages.extend((0..count).map(|index| Message {
                bytes: object.datagram(index),
                segment: 0,
            }));
        }
    }

    /// Send `messages` to `peer`. Returns how many were handed to the kernel
    /// and the error that stopped the rest, if any. Before each call to the
    /// kernel, `call_times` gets the index of the call's first message and
    /// the wall-clock time in microseconds.
    pub async fn send(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        messages: &[Message],
        call_times: &mut Vec<(usize, u64)>,
    ) -> (usize, Option<io::Error>) {
        #[cfg(target_os = "linux")]
        {
            self.send_batched(socket, peer, messages, call_times).await
        }
        #[cfg(not(target_os = "linux"))]
        {
            send_one_by_one(socket, peer, messages, call_times).await
        }
    }

    #[cfg(target_os = "linux")]
    async fn send_batched(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        messages: &[Message],
        call_times: &mut Vec<(usize, u64)>,
    ) -> (usize, Option<io::Error>) {
        use tokio::io::Interest;
        let address = socket2::SockAddr::from(peer);
        let mut sent = 0;
        while sent < messages.len() {
            let now = unix_micros();
            let result = socket.try_io(Interest::WRITABLE, || {
                linux::sendmmsg(socket, &address, &messages[sent..], self.gso)
            });
            match result {
                Ok(0) => {
                    return (sent, Some(io::Error::other("sendmmsg took no message")));
                }
                Ok(count) => {
                    call_times.push((sent, now));
                    sent += count.min(messages.len() - sent);
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(err) = socket.writable().await {
                        return (sent, Some(err));
                    }
                }
                Err(err) if self.gso && messages[sent].segment > 0 && linux::gso_refused(&err) => {
                    // The path cannot segment: send the rest one datagram
                    // at a time from now on.
                    tracing::warn!(error = %err, "cache mesh UDP GSO refused; sending datagrams one by one");
                    self.gso = false;
                    let rest = split(&messages[sent..]);
                    let mut times = Vec::new();
                    let (done, err) =
                        Box::pin(self.send_batched(socket, peer, &rest, &mut times)).await;
                    // Map the progress over the split datagrams back to the
                    // messages they came from.
                    let mut datagrams = 0;
                    for (offset, message) in messages[sent..].iter().enumerate() {
                        if datagrams == 0 {
                            if let Some((_, at)) = times.first() {
                                call_times.push((sent + offset, *at));
                            }
                        }
                        datagrams += message.datagrams();
                        if datagrams > done {
                            return (sent + offset, err);
                        }
                    }
                    return (messages.len(), err);
                }
                Err(err) => return (sent, Some(err)),
            }
        }
        (sent, None)
    }
}

/// The datagrams of `messages`, one message each.
#[cfg(target_os = "linux")]
fn split(messages: &[Message]) -> Vec<Message> {
    let mut out = Vec::new();
    for message in messages {
        if message.segment == 0 {
            out.push(message.clone());
            continue;
        }
        let mut start = 0;
        while start < message.bytes.len() {
            let end = (start + message.segment).min(message.bytes.len());
            out.push(Message {
                bytes: message.bytes.slice(start..end),
                segment: 0,
            });
            start = end;
        }
    }
    out
}

/// Send each datagram with its own call: the fallback where there is no
/// `sendmmsg`.
#[cfg_attr(target_os = "linux", allow(dead_code))]
async fn send_one_by_one(
    socket: &UdpSocket,
    peer: SocketAddr,
    messages: &[Message],
    call_times: &mut Vec<(usize, u64)>,
) -> (usize, Option<io::Error>) {
    for (index, message) in messages.iter().enumerate() {
        call_times.push((index, unix_micros()));
        let segment = if message.segment == 0 {
            message.bytes.len()
        } else {
            message.segment
        };
        for datagram in message.bytes.chunks(segment.max(1)) {
            if let Err(err) = socket.send_to(datagram, peer).await {
                return (index, Some(err));
            }
        }
    }
    (messages.len(), None)
}

/// Whether the kernel segments UDP datagrams for this socket.
fn gso_supported(socket: &UdpSocket) -> bool {
    #[cfg(target_os = "linux")]
    {
        linux::gso_supported(socket)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = socket;
        false
    }
}

/// The time at which a message was handed to the kernel: that of the call
/// that carried it.
pub(crate) fn call_time(call_times: &[(usize, u64)], message: usize) -> u64 {
    let position = call_times.partition_point(|(first, _)| *first <= message);
    call_times
        .get(position.saturating_sub(1))
        .map_or_else(unix_micros, |(_, at)| *at)
}

fn unix_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_micros() as u64)
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{Message, MESSAGES_PER_CALL};
    use std::io;
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd;
    use tokio::net::UdpSocket;

    /// Room for one control message that carries a `u16`.
    #[repr(C, align(8))]
    #[derive(Clone, Copy)]
    struct Control([u8; 32]);

    pub fn gso_supported(socket: &UdpSocket) -> bool {
        let mut value: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: the pointers are to locals of the sizes passed.
        let rc = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_UDP,
                libc::UDP_SEGMENT,
                (&mut value as *mut libc::c_int).cast(),
                &mut len,
            )
        };
        rc == 0
    }

    /// Errors with which a kernel or device refuses a segmented send.
    pub fn gso_refused(err: &io::Error) -> bool {
        matches!(
            err.raw_os_error(),
            Some(libc::EIO | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOPROTOOPT)
        )
    }

    /// One `sendmmsg` call for up to [`MESSAGES_PER_CALL`] messages. Returns
    /// how many the kernel took.
    pub fn sendmmsg(
        socket: &UdpSocket,
        address: &socket2::SockAddr,
        messages: &[Message],
        gso: bool,
    ) -> io::Result<usize> {
        let count = messages.len().min(MESSAGES_PER_CALL);
        if count == 0 {
            return Ok(0);
        }
        // SAFETY: zeroed iovec, mmsghdr and control buffers are valid; every
        // pointer stored below is to memory that outlives the call, and the
        // kernel only reads the buffers of a send.
        unsafe {
            let mut iovecs: [MaybeUninit<libc::iovec>; MESSAGES_PER_CALL] =
                MaybeUninit::uninit().assume_init();
            let mut headers: [MaybeUninit<libc::mmsghdr>; MESSAGES_PER_CALL] =
                MaybeUninit::uninit().assume_init();
            let mut controls = [Control([0; 32]); MESSAGES_PER_CALL];
            for (index, message) in messages[..count].iter().enumerate() {
                iovecs[index].write(libc::iovec {
                    iov_base: message.bytes.as_ptr() as *mut libc::c_void,
                    iov_len: message.bytes.len(),
                });
                let mut header: libc::mmsghdr = std::mem::zeroed();
                header.msg_hdr.msg_name = address.as_ptr() as *mut libc::c_void;
                header.msg_hdr.msg_namelen = address.len();
                header.msg_hdr.msg_iov = iovecs[index].as_mut_ptr();
                header.msg_hdr.msg_iovlen = 1;
                if gso && message.segment > 0 && message.bytes.len() > message.segment {
                    let space = libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) as usize;
                    header.msg_hdr.msg_control = controls[index].0.as_mut_ptr().cast();
                    header.msg_hdr.msg_controllen = space as _;
                    let control = libc::CMSG_FIRSTHDR(&header.msg_hdr);
                    (*control).cmsg_level = libc::SOL_UDP;
                    (*control).cmsg_type = libc::UDP_SEGMENT;
                    (*control).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as _;
                    std::ptr::write_unaligned(
                        libc::CMSG_DATA(control).cast::<u16>(),
                        message.segment as u16,
                    );
                }
                headers[index].write(header);
            }
            let rc = libc::sendmmsg(
                socket.as_raw_fd(),
                headers.as_mut_ptr().cast::<libc::mmsghdr>(),
                count as libc::c_uint,
                0,
            );
            if rc < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(rc as usize)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq_datagram_fec::{DatagramFecDecoder, DatagramFecEncoder};

    #[test]
    fn planned_encoding_is_the_datagram_fec_encoding() {
        let mut reference = DatagramFecEncoder::new().with_initial_block_id(77);
        let mut planned = ObjectEncoder::new(77);
        for (len, symbol_size, repair) in [
            (1_usize, 1_316_u16, 1_u32),
            (40, 1_316, 1),
            (300, 1_316, 0),
            (1_316, 1_316, 2),
            (1_317, 1_316, 1),
            (8_000, 1_316, 3),
            (100_000, 1_316, 4),
            (100_000, 8_100, 2),
            (40, 1_316, 1),
            (120, 120, 5),
            (1_316, 1_316, 4),
            (7, 7, 1),
        ] {
            // Every byte value, in no simple order.
            let frame = (0..len)
                .map(|index| (index * 167 + len * 13 + index / 256) as u8)
                .collect::<Vec<_>>();
            reference.set_symbol_size(symbol_size);
            let expected = reference
                .encode_object_with_repair_symbols(&frame, repair)
                .unwrap();
            let (head, tail) = frame.split_at(len / 3);
            let got = planned.encode(&[head, tail], symbol_size, repair).unwrap();
            assert_eq!(got.count(), expected.len(), "{len} bytes");
            for (index, datagram) in expected.iter().enumerate() {
                assert_eq!(
                    &got.datagram(index)[..],
                    &datagram[..],
                    "{len} bytes, datagram {index}"
                );
            }
            assert_eq!(
                got.source_symbols,
                usize::from(source_symbol_count(len, symbol_size))
            );
        }
        // One plan for each source-symbol count that needed repair.
        assert_eq!(planned.plans(), 5);
    }

    #[test]
    fn planned_encoding_decodes_from_repair_alone() {
        let mut planned = ObjectEncoder::new(5);
        let frame = (0..5_000).map(|index| index as u8).collect::<Vec<_>>();
        let object = planned.encode(&[&frame], 1_316, 4).unwrap();
        assert_eq!(object.count(), 8);
        let mut decoder = DatagramFecDecoder::new();
        let mut decoded = None;
        // Lose the first source symbol and the third.
        for index in [1, 3, 4, 5, 6] {
            if let Some(frame) = decoder.push_datagram(&object.datagram(index)).unwrap() {
                decoded = Some(frame);
            }
        }
        assert_eq!(decoded.unwrap(), frame);
    }

    #[test]
    fn a_large_object_is_cut_into_segmented_messages() {
        let gso = BatchSender { gso: true };
        let no_gso = BatchSender { gso: false };
        let mut planned = ObjectEncoder::new(0);
        let object = planned.encode(&[&vec![1_u8; 200_000]], 1_316, 2).unwrap();
        assert_eq!(object.datagram_len, 1_352);
        let count = object.count();
        let mut messages = Vec::new();
        gso.push(&mut messages, &object, count);
        // 48 datagrams of 1352 bytes fit 65000.
        assert_eq!(messages.len(), count.div_ceil(48));
        assert!(messages.iter().all(|message| message.segment == 1_352));
        assert_eq!(
            messages.iter().map(Message::datagrams).sum::<usize>(),
            count
        );
        messages.clear();
        no_gso.push(&mut messages, &object, 10);
        assert_eq!(messages.len(), 10);
        assert!(messages.iter().all(|message| message.segment == 0));
        // A symbol too large for one packet is never segmented.
        let large = planned.encode(&[&vec![1_u8; 200_000]], 8_100, 1).unwrap();
        messages.clear();
        gso.push(&mut messages, &large, large.count());
        assert_eq!(messages.len(), large.count());
    }

    #[test]
    fn a_message_takes_the_time_of_the_call_that_carried_it() {
        let times = [(0, 10), (64, 20), (128, 30)];
        assert_eq!(call_time(&times, 0), 10);
        assert_eq!(call_time(&times, 63), 10);
        assert_eq!(call_time(&times, 64), 20);
        assert_eq!(call_time(&times, 200), 30);
    }

    async fn receive_all(socket: &UdpSocket, expected: usize) -> Vec<Vec<u8>> {
        let mut received = Vec::new();
        let mut buf = vec![0_u8; 65_536];
        while received.len() < expected {
            let (len, _) = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                socket.recv_from(&mut buf),
            )
            .await
            .expect("a datagram did not arrive")
            .unwrap();
            received.push(buf[..len].to_vec());
        }
        received
    }

    #[tokio::test]
    async fn a_batch_arrives_as_the_datagrams_it_was_made_of() {
        let rx = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let _ = socket2::SockRef::from(&rx).set_recv_buffer_size(4 << 20);
        let tx = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer = rx.local_addr().unwrap();
        let mut planned = ObjectEncoder::new(0);
        for gso in [true, false] {
            let mut sender = BatchSender::new(&tx, gso);
            let mut messages = Vec::new();
            let mut expected = Vec::new();
            // Small audio parts and one video part, more than one call holds.
            for part in 0..100_usize {
                let len = if part == 50 { 60_000 } else { 40 };
                let object = planned.encode(&[&vec![part as u8; len]], 1_316, 1).unwrap();
                sender.push(&mut messages, &object, object.count());
                expected.extend((0..object.count()).map(|index| object.datagram(index).to_vec()));
            }
            let mut times = Vec::new();
            let (sent, err) = sender.send(&tx, peer, &messages, &mut times).await;
            assert!(err.is_none(), "{err:?}");
            assert_eq!(sent, messages.len());
            assert!(times.len() >= messages.len().div_ceil(MESSAGES_PER_CALL));
            let received = receive_all(&rx, expected.len()).await;
            assert_eq!(received, expected, "gso {}", sender.gso());
        }
    }

    /// The cost of encoding a small part, with the plan kept and without.
    #[test]
    #[ignore = "measurement: cargo test --release -- --ignored encode_cost"]
    fn encode_cost_of_a_small_part() {
        let frame = vec![7_u8; 120];
        let rounds = 20_000;
        let mut reference = DatagramFecEncoder::new();
        let start = std::time::Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(
                reference
                    .encode_object_with_repair_symbols(&frame, 1)
                    .unwrap(),
            );
        }
        let without = start.elapsed() / rounds;
        let mut planned = ObjectEncoder::new(0);
        let start = std::time::Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(planned.encode(&[&frame], 1_316, 1).unwrap());
        }
        let with = start.elapsed() / rounds;
        let start = std::time::Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(planned.encode(&[&frame], 120, 1).unwrap());
        }
        let small = start.elapsed() / rounds;
        eprintln!("encode of a 120-byte part with one repair symbol: {without:?} without a kept plan, {with:?} with, {small:?} with a symbol of the part's size");
    }
}
