package homekit

import (
	"bufio"
	"fmt"
	"io"
	"log"
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
	// Prefer an ffmpeg with libfdk_aac that can actually encode AAC-ELD.
	// The system libfdk-aac package may not support ELD with RAW transport,
	// so we test actual encoding rather than just checking -encoders.
	// Recommended: install homebridge static ffmpeg which bundles working libfdk-aac:
	//   curl -Lf https://github.com/homebridge/ffmpeg-for-homebridge/releases/latest/download/ffmpeg-alpine-x86_64.tar.gz | tar xzf - -C /tmp && cp /tmp/usr/local/bin/ffmpeg /usr/local/bin/ffmpeg-homebridge
	ffmpegBin := ""
	ffmpegCodec := ""
	for _, bin := range []string{"/usr/local/bin/ffmpeg-homebridge", "/usr/local/bin/ffmpeg-fdk", "ffmpeg-fdk", "ffmpeg"} {
		// Actually test AAC-ELD encoding — some libfdk-aac versions fail at init
		testCmd := shell.NewCommand(bin + " -hide_banner -loglevel error -f lavfi -i sine=frequency=440:duration=0.1 -c:a libfdk_aac -profile:a aac_eld -ar 16000 -ac 1 -f null /dev/null")
		if err := testCmd.Run(); err == nil {
			ffmpegBin = bin
			ffmpegCodec = "-c:a libfdk_aac -profile:a aac_eld"
			log.Printf("[backchannel] using %s with libfdk_aac (ELD test passed)", bin)
			break
		}
	}
	if ffmpegBin == "" {
		// Fall back to native AAC encoder (produces 1024-sample frames,
		// split into individual frames in readAndSend)
		ffmpegBin = "ffmpeg"
		ffmpegCodec = "-c:a aac -profile:a aac_eld"
		log.Printf("[backchannel] WARNING: no ffmpeg with working libfdk_aac ELD found, using native encoder")
	}

	ffmpegCmd := fmt.Sprintf(
		"%s -hide_banner -loglevel error"+
			" -protocol_whitelist file,rtp,udp"+
			" -f sdp -i %s"+
			" %s -ar 16000 -ac 1 -b:a 32k"+
			" -f rtp rtp://127.0.0.1:%d",
		ffmpegBin, sdpFileName, ffmpegCodec, outputPort,
	)

	log.Printf("[backchannel] ffmpeg cmd: %s", ffmpegCmd)
	log.Printf("[backchannel] SDP content:\n%s", sdp)

	cmd := shell.NewCommand(ffmpegCmd)

	// Capture stderr for debugging
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

	// Log ffmpeg stderr in background
	if stderr != nil {
		go func() {
			scanner := bufio.NewScanner(stderr)
			for scanner.Scan() {
				log.Printf("[backchannel] ffmpeg stderr: %s", scanner.Text())
			}
		}()
	}

	// Log when ffmpeg exits
	go func() {
		<-cmd.Done()
		log.Printf("[backchannel] ffmpeg process exited")
	}()

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
	var handlerCount int
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
		handlerCount++

		if handlerCount <= 3 {
			log.Printf("[backchannel] handler sending Opus packet #%d to ffmpeg: payloadLen=%d ts=%d",
				handlerCount, len(packet.Payload), packet.Timestamp)
		}

		b, err := clone.Marshal()
		if err != nil {
			return
		}

		if _, err := sendConn.Write(b); err != nil && handlerCount <= 3 {
			log.Printf("[backchannel] handler Write error: %v", err)
		}
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
// ffmpeg's native AAC encoder with ELD profile produces 1024-sample frames
// packed multiple per RTP packet (RFC 3640 with AU headers). HomeKit cameras
// expect single AAC-ELD frames per RTP packet with 480-sample timestamps.
// We unpack the multi-frame packets and send each frame individually with
// corrected timestamps.
func (p *backchannelPipeline) readAndSend(session *srtp.Session, sendCounter *int) {
	buf := make([]byte, 2048)

	// HomeKit expects 480-sample timestamp increments (30ms at 16kHz)
	const timestampIncrement = 480
	var timestamp uint32
	var seq uint16
	var totalRead int

	log.Printf("[backchannel] readAndSend started, listening on %s", p.udpConn.LocalAddr())

	for {
		n, _, err := p.udpConn.ReadFrom(buf)
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if !closed {
				log.Printf("[backchannel] readAndSend unexpected error: %v", err)
			}
			log.Printf("[backchannel] readAndSend exiting, totalRead=%d", totalRead)
			return
		}

		totalRead++
		if totalRead <= 3 {
			log.Printf("[backchannel] readAndSend got packet #%d: %d bytes", totalRead, n)
		}

		// Parse the RTP packet from ffmpeg
		packet := &rtp.Packet{}
		if err := packet.Unmarshal(buf[:n]); err != nil {
			log.Printf("[backchannel] readAndSend unmarshal error: %v", err)
			continue
		}

		// Unpack RFC 3640 AU headers and send each AAC frame individually
		payload := packet.Payload
		if len(payload) < 4 {
			if totalRead <= 3 {
				log.Printf("[backchannel] readAndSend payload too short: %d", len(payload))
			}
			continue
		}

		// First 2 bytes: AU-headers-length in bits
		auHeadersLenBits := int(payload[0])<<8 | int(payload[1])
		auHeadersLen := (auHeadersLenBits + 7) / 8 // round up to bytes
		numFrames := auHeadersLen / 2               // each AU header is 16 bits (13 size + 3 index)

		if numFrames == 0 || len(payload) < 2+auHeadersLen {
			continue
		}

		// Parse AU sizes from headers
		headers := payload[2 : 2+auHeadersLen]
		data := payload[2+auHeadersLen:]

		if totalRead <= 3 {
			log.Printf("[backchannel] readAndSend PT=%d numFrames=%d auHeadersLen=%d dataLen=%d",
				packet.PayloadType, numFrames, auHeadersLen, len(data))
		}

		for i := 0; i < numFrames && len(headers) >= 2; i++ {
			auSize := (int(headers[0])<<8 | int(headers[1])) >> 3 // 13-bit size
			headers = headers[2:]

			if auSize > len(data) {
				log.Printf("[backchannel] readAndSend auSize %d > dataLen %d", auSize, len(data))
				break
			}

			frame := data[:auSize]
			data = data[auSize:]

			// Wrap single frame in RFC 3640 format (single AU)
			singlePayload := make([]byte, 4+len(frame))
			singlePayload[0] = 0x00
			singlePayload[1] = 0x10                                           // 16 bits of AU headers
			singlePayload[2] = byte((len(frame) << 3) >> 8)                   // AU size high bits
			singlePayload[3] = byte((len(frame) << 3) & 0xFF)                 // AU size low bits
			copy(singlePayload[4:], frame)

			singlePacket := &rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					Marker:         true,
					SequenceNumber: seq,
					Timestamp:      timestamp,
				},
				Payload: singlePayload,
			}
			seq++
			timestamp += timestampIncrement

			if sent, err := session.WriteRTP(singlePacket); err == nil {
				*sendCounter += sent
			} else if totalRead <= 3 {
				log.Printf("[backchannel] readAndSend WriteRTP error: %v", err)
			}
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
