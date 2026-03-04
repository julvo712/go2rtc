package homekit

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/shell"
	"github.com/AlexxIT/go2rtc/pkg/srtp"
	"github.com/pion/rtp"
)

// backchannelPipeline manages the ffmpeg transcoding subprocess that converts
// incoming Opus RTP packets (from WebRTC) to AAC-ELD RTP packets, which are
// then sent to the HomeKit camera via SRTP.
type backchannelPipeline struct {
	cmd      *shell.Command
	sendConn *net.UDPConn   // sends Opus RTP to ffmpeg input
	udpConn  net.PacketConn // receives AAC-ELD RTP from ffmpeg output
	sdpFile  string         // temp SDP file path
	mu       sync.Mutex
	closed   bool
}

// startBackchannelPipeline spawns an ffmpeg process that:
// 1. Receives Opus RTP on a localhost UDP port
// 2. Transcodes Opus → AAC-ELD (16kHz mono)
// 3. Outputs AAC-ELD RTP to another localhost UDP port
//
// It returns the pipeline and a handler function for incoming Opus RTP packets.
// A goroutine reads AAC-ELD RTP output and sends it to the camera via SRTP.
func startBackchannelPipeline(session *srtp.Session, sendCounter *int) (*backchannelPipeline, core.HandlerFunc, error) {
	// Open a UDP port for ffmpeg to listen on for Opus RTP input.
	// We bind it, get the port, then close — ffmpeg will bind it when it starts.
	inputAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: resolve: %w", err)
	}
	inputTmp, err := net.ListenUDP("udp", inputAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: listen input: %w", err)
	}
	inputPort := inputTmp.LocalAddr().(*net.UDPAddr).Port
	inputTmp.Close() // release so ffmpeg can bind it

	// Open a UDP port for receiving AAC-ELD RTP output from ffmpeg
	outputConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: listen output: %w", err)
	}
	outputPort := outputConn.LocalAddr().(*net.UDPAddr).Port

	// Create SDP file describing the Opus RTP input stream
	sdp := fmt.Sprintf(
		"v=0\r\n"+
			"o=- 0 0 IN IP4 127.0.0.1\r\n"+
			"s=backchannel\r\n"+
			"c=IN IP4 127.0.0.1\r\n"+
			"t=0 0\r\n"+
			"m=audio %d RTP/AVP 111\r\n"+
			"a=rtpmap:111 opus/48000/2\r\n",
		inputPort,
	)

	sdpFile, err := os.CreateTemp("", "go2rtc-backchannel-*.sdp")
	if err != nil {
		outputConn.Close()
		return nil, nil, fmt.Errorf("backchannel: create sdp: %w", err)
	}
	sdpFileName := sdpFile.Name()
	if _, err := sdpFile.WriteString(sdp); err != nil {
		outputConn.Close()
		sdpFile.Close()
		os.Remove(sdpFileName)
		return nil, nil, fmt.Errorf("backchannel: write sdp: %w", err)
	}
	sdpFile.Close()

	// Spawn ffmpeg to transcode Opus RTP → AAC-ELD RTP
	ffmpegCmd := fmt.Sprintf(
		"ffmpeg -hide_banner -loglevel error"+
			" -protocol_whitelist file,rtp,udp"+
			" -f sdp -i %s"+
			" -c:a aac -profile:a aac_eld -ar 16000 -ac 1 -b:a 24k"+
			" -f rtp rtp://127.0.0.1:%d",
		sdpFileName, outputPort,
	)

	cmd := shell.NewCommand(ffmpegCmd)

	// Capture stderr for debugging (drain it to avoid blocking ffmpeg)
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		outputConn.Close()
		os.Remove(sdpFileName)
		return nil, nil, fmt.Errorf("backchannel: start ffmpeg: %w", err)
	}

	pipeline := &backchannelPipeline{
		cmd:     cmd,
		udpConn: outputConn,
		sdpFile: sdpFileName,
	}

	// Drain ffmpeg stderr in background
	if stderr != nil {
		go func() {
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				// stderr is drained; messages visible if debug logging is added
			}
		}()
	}

	// Start goroutine to read AAC-ELD RTP from ffmpeg output and send to camera
	go pipeline.readAndSend(session, sendCounter)

	// Create UDP connection to send Opus RTP to ffmpeg's input port
	ffmpegAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", inputPort))
	sendConn, err := net.DialUDP("udp", nil, ffmpegAddr)
	if err != nil {
		pipeline.Close()
		return nil, nil, fmt.Errorf("backchannel: dial ffmpeg: %w", err)
	}
	pipeline.sendConn = sendConn

	var seq uint16
	handler := func(packet *rtp.Packet) {
		// Re-create a clean RTP packet with the correct payload type
		// matching our SDP (PT 111 = Opus)
		clone := rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    111,
				SequenceNumber: seq,
				Timestamp:      packet.Timestamp,
				SSRC:           0x12345678,
				Marker:         packet.Marker,
			},
			Payload: packet.Payload,
		}
		seq++

		b, err := clone.Marshal()
		if err != nil {
			return
		}

		sendConn.Write(b)
	}

	// Clean up send socket when ffmpeg exits
	go func() {
		<-cmd.Done()
		sendConn.Close()
	}()

	return pipeline, handler, nil
}

// readAndSend reads AAC-ELD RTP packets from ffmpeg's output UDP port
// and forwards them to the HomeKit camera via SRTP.
//
// ffmpeg outputs AAC in standard RFC 3640 RTP format, which is what
// HomeKit cameras expect for AAC-ELD. We forward the payload as-is
// through session.WriteRTP which handles SRTP encryption, SSRC, and
// PayloadType injection.
func (p *backchannelPipeline) readAndSend(session *srtp.Session, sendCounter *int) {
	buf := make([]byte, 2048)

	var debugCount int

	for {
		n, _, err := p.udpConn.ReadFrom(buf)
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if !closed {
				// Unexpected read error — pipeline may need restart
			}
			return
		}

		// Parse the RTP packet from ffmpeg
		packet := &rtp.Packet{}
		if err := packet.Unmarshal(buf[:n]); err != nil {
			continue
		}

		// Debug: log first few packets to verify format
		if debugCount < 5 {
			fmt.Printf("[backchannel] ffmpeg RTP: PT=%d SSRC=%08x TS=%d Seq=%d PayloadLen=%d Payload[:8]=%x\n",
				packet.PayloadType, packet.SSRC, packet.Timestamp, packet.SequenceNumber,
				len(packet.Payload), packet.Payload[:min(8, len(packet.Payload))])
			fmt.Printf("[backchannel] session: PT=%d LocalSSRC=%08x RemoteAddr=%s\n",
				session.PayloadType, session.Local.SSRC, session.Remote.Addr+":"+fmt.Sprint(session.Remote.Port))
			debugCount++
		}

		// Forward to camera via SRTP
		if sent, err := session.WriteRTP(packet); err == nil {
			*sendCounter += sent
		}
	}
}

// Close shuts down the ffmpeg process and cleans up resources.
func (p *backchannelPipeline) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if p.sendConn != nil {
		p.sendConn.Close()
	}
	if p.udpConn != nil {
		p.udpConn.Close()
	}
	if p.cmd != nil {
		p.cmd.Close()
	}
	if p.sdpFile != "" {
		os.Remove(p.sdpFile)
	}
	return nil
}

// readADTSFrame reads a single ADTS frame from a reader.
// Currently unused but kept for potential pipe-based approach.
func readADTSFrame(r *bufio.Reader) ([]byte, error) {
	header := make([]byte, 7) // ADTS header is 7 bytes
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	// Verify sync word
	if header[0] != 0xFF || header[1]&0xF0 != 0xF0 {
		return nil, fmt.Errorf("backchannel: not ADTS sync: %x%x", header[0], header[1])
	}

	// Extract frame size from ADTS header bits
	frameSize := int(header[3]&0x03)<<11 | int(header[4])<<3 | int(header[5]>>5)
	if frameSize < 7 {
		return nil, fmt.Errorf("backchannel: invalid ADTS frame size: %d", frameSize)
	}

	// Read the remaining payload (frameSize includes the 7-byte header)
	payload := make([]byte, frameSize-7)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	return payload, nil
}
