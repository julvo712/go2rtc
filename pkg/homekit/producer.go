package homekit

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/hap"
	"github.com/AlexxIT/go2rtc/pkg/hap/camera"
	"github.com/AlexxIT/go2rtc/pkg/srtp"
	"github.com/pion/rtp"
)

// Deprecated: rename to Producer
type Client struct {
	core.Connection

	hap  *hap.Client
	srtp *srtp.Server

	videoConfig camera.SupportedVideoStreamConfiguration
	audioConfig camera.SupportedAudioStreamConfiguration

	videoSession *srtp.Session
	audioSession *srtp.Session

	stream *camera.Stream

	backchannel      *backchannelPipeline // ffmpeg Opus→AAC-ELD transcoder
	backchannelCodec *core.Codec          // codec of the backchannel track (for deferred start)
	backchannelTrack *core.Receiver       // backchannel track (for deferred start)

	forwardAudio *forwardAudioPipeline // ffmpeg ELD→Opus transcoder (camera mic → browser)

	MaxWidth  int `json:"-"`
	MaxHeight int `json:"-"`
	Bitrate   int `json:"-"` // in bits/s
}

func Dial(rawURL string, server *srtp.Server) (*Client, error) {
	conn, err := hap.Dial(rawURL)
	if err != nil {
		return nil, err
	}

	client := &Client{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "homekit",
			Protocol:   "udp",
			RemoteAddr: conn.Conn.RemoteAddr().String(),
			Source:     rawURL,
			Transport:  conn,
		},
		hap:  conn,
		srtp: server,
	}

	return client, nil
}

func (c *Client) Conn() net.Conn {
	return c.hap.Conn
}

func (c *Client) GetMedias() []*core.Media {
	if c.Medias != nil {
		return c.Medias
	}

	acc, err := c.hap.GetFirstAccessory()
	if err != nil {
		return nil
	}

	char := acc.GetCharacter(camera.TypeSupportedVideoStreamConfiguration)
	if char == nil {
		return nil
	}
	if err = char.ReadTLV8(&c.videoConfig); err != nil {
		return nil
	}

	char = acc.GetCharacter(camera.TypeSupportedAudioStreamConfiguration)
	if char == nil {
		return nil
	}
	if err = char.ReadTLV8(&c.audioConfig); err != nil {
		return nil
	}

	c.SDP = fmt.Sprintf("%+v\n%+v", c.videoConfig, c.audioConfig)

	// Recvonly audio: camera's native codecs (ELD) plus Opus for transcoded output.
	// If a consumer only speaks Opus (e.g. WebRTC), we transcode ELD→Opus in Start().
	recvAudio := audioToMedia(c.audioConfig.Codecs, core.DirectionRecvonly)
	recvAudio.Codecs = append(recvAudio.Codecs, &core.Codec{
		Name:      core.CodecOpus,
		ClockRate: 48000,
		Channels:  2,
	})

	c.Medias = []*core.Media{
		videoToMedia(c.videoConfig.Codecs),
		recvAudio,
		{
			Kind:      core.KindVideo,
			Direction: core.DirectionRecvonly,
			Codecs: []*core.Codec{
				{
					Name:        core.CodecJPEG,
					ClockRate:   90000,
					PayloadType: core.PayloadTypeRAW,
				},
			},
		},
	}

	// If the camera has a Speaker service, advertise sendonly audio media
	// so that backchannel audio (e.g. from WebRTC mic) can be routed to
	// the camera's speaker via the existing SRTP session.
	// We advertise both the camera's native codec (AAC-ELD) and Opus,
	// since the ffmpeg transcoding pipeline in startBackchannel() can
	// convert Opus → AAC-ELD. This allows WebRTC consumers (which
	// typically send Opus) to match the backchannel media.
	if acc.GetService(camera.TypeSpeaker) != nil {
		backchannel := audioToMedia(c.audioConfig.Codecs, core.DirectionSendonly)
		backchannel.Codecs = append(backchannel.Codecs, &core.Codec{
			Name:      core.CodecOpus,
			ClockRate: 48000,
			Channels:  2,
		})
		c.Medias = append(c.Medias, backchannel)
	}

	return c.Medias
}

func (c *Client) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	if media.Direction != core.DirectionSendonly {
		return errors.New("homekit: AddTrack only for sendonly (backchannel)")
	}

	c.backchannelCodec = codec
	c.backchannelTrack = track

	// If Start() already ran (audioSession exists), start backchannel immediately.
	// This handles the case where a mic consumer (e.g. WebRTC) connects after
	// a non-mic consumer (e.g. MSE) already triggered Start().
	if c.audioSession != nil {
		if err := c.startBackchannel(); err != nil {
			return fmt.Errorf("homekit: late backchannel start: %w", err)
		}
	}

	return nil
}

// unmuteSpeaker sets the Speaker service's Mute characteristic to false.
// HomeKit cameras may default to muted; without this the camera ignores
// incoming audio even though SRTP packets are delivered correctly.
func (c *Client) unmuteSpeaker() {
	acc, err := c.hap.GetFirstAccessory()
	if err != nil {
		log.Printf("[homekit] unmute: failed to get accessory: %v", err)
		return
	}

	speaker := acc.GetService("113") // TypeSpeaker
	if speaker == nil {
		log.Printf("[homekit] unmute: no speaker service")
		return
	}

	muteChar := speaker.GetCharacter("11A") // Mute
	if muteChar == nil {
		log.Printf("[homekit] unmute: no mute characteristic")
		return
	}

	muteChar.Value = false
	if err := c.hap.PutCharacters(muteChar); err != nil {
		log.Printf("[homekit] unmute: failed to set mute=false: %v", err)
		return
	}
	log.Printf("[homekit] unmuted speaker (IID=%d)", muteChar.IID)

	// Also try setting volume if available
	volChar := speaker.GetCharacter("119") // Volume
	if volChar != nil {
		volChar.Value = 100
		if err := c.hap.PutCharacters(volChar); err != nil {
			log.Printf("[homekit] volume: failed to set: %v", err)
		} else {
			log.Printf("[homekit] set speaker volume to 100 (IID=%d)", volChar.IID)
		}
	}
}

// startBackchannel wires up the backchannel audio pipeline after the SRTP
// session has been established. Must be called from Start().
func (c *Client) startBackchannel() error {
	// Unmute the camera speaker before starting backchannel audio
	c.unmuteSpeaker()

	codec := c.backchannelCodec
	track := c.backchannelTrack

	media := &core.Media{
		Kind:      core.KindAudio,
		Direction: core.DirectionSendonly,
		Codecs:    []*core.Codec{codec},
	}

	sender := core.NewSender(media, track.Codec)

	// The camera only speaks AAC-ELD. If the incoming track is already AAC-ELD,
	// we can forward directly. Otherwise we need ffmpeg transcoding (typically
	// Opus from WebRTC → AAC-ELD for the camera).
	if codec.Name == core.CodecELD || codec.Name == core.CodecAAC {
		// Direct path: incoming AAC-ELD → SRTP → camera
		sender.Handler = func(packet *rtp.Packet) {
			if n, err := c.audioSession.WriteRTP(packet); err == nil {
				c.Send += n
			}
		}
	} else {
		// Transcoding path: spawn ffmpeg Opus→AAC-ELD pipeline
		pipeline, handler, err := startBackchannelPipeline(c.audioSession, &c.Send)
		if err != nil {
			return fmt.Errorf("homekit: backchannel pipeline: %w", err)
		}
		c.backchannel = pipeline
		sender.Handler = handler
	}

	sender.HandleRTP(track)
	c.Senders = append(c.Senders, sender)
	return nil
}

func (c *Client) Start() error {
	if c.Receivers == nil {
		return errors.New("producer without tracks")
	}

	if c.Receivers[0].Codec.Name == core.CodecJPEG {
		return c.startMJPEG()
	}

	videoTrack := c.trackByKind(core.KindVideo)
	videoCodec := trackToVideo(videoTrack, &c.videoConfig.Codecs[0], c.MaxWidth, c.MaxHeight)

	audioTrack := c.trackByKind(core.KindAudio)
	// Always negotiate camera's native ELD codec, regardless of consumer codec.
	// If the consumer wants Opus, we transcode ELD→Opus via ffmpeg below.
	audioCodec := trackToAudio(nil, &c.audioConfig.Codecs[0])

	c.videoSession = &srtp.Session{Local: c.srtpEndpoint()}
	c.audioSession = &srtp.Session{Local: c.srtpEndpoint()}

	var err error
	c.stream, err = camera.NewStream(c.hap, videoCodec, audioCodec, c.videoSession, c.audioSession, c.Bitrate)
	if err != nil {
		return err
	}

	c.srtp.AddSession(c.videoSession)
	c.srtp.AddSession(c.audioSession)

	// Store PayloadType and RTCPInterval so that WriteRTP (backchannel)
	// uses the correct values when sending audio to the camera.
	c.videoSession.PayloadType = videoCodec.RTPParams[0].PayloadType
	c.videoSession.RTCPInterval = toDuration(videoCodec.RTPParams[0].RTCPInterval)
	c.audioSession.PayloadType = audioCodec.RTPParams[0].PayloadType
	c.audioSession.RTCPInterval = toDuration(audioCodec.RTPParams[0].RTCPInterval)

	// Start backchannel pipeline if a backchannel track was registered via AddTrack
	if c.backchannelTrack != nil {
		if err := c.startBackchannel(); err != nil {
			// Backchannel failure is non-fatal — video/audio reception still works
			_ = err
		}
	}

	deadline := time.NewTimer(core.ConnDeadline)

	// Set up video handler
	if videoTrack != nil {
		c.videoSession.OnReadRTP = func(packet *rtp.Packet) {
			deadline.Reset(core.ConnDeadline)
			videoTrack.WriteRTP(packet)
			c.Recv += len(packet.Payload)
		}
	}

	// Set up audio handler (with ELD→Opus transcoding if needed)
	if audioTrack != nil {
		needsDeadline := videoTrack == nil
		needsTranscoding := audioTrack.Codec.Name == core.CodecOpus

		if needsTranscoding {
			log.Printf("[homekit] audio track is Opus, setting up ELD→Opus transcoding")
			fwd, err := startForwardAudioPipeline(audioTrack, &c.Recv)
			if err != nil {
				log.Printf("[homekit] forward audio failed: %v", err)
			} else {
				c.forwardAudio = fwd
				c.audioSession.OnReadRTP = func(packet *rtp.Packet) {
					if needsDeadline {
						deadline.Reset(core.ConnDeadline)
					}
					fwd.WriteELDPacket(packet)
					c.Recv += len(packet.Payload)
				}
			}
		} else {
			// Direct ELD passthrough (consumer natively supports ELD)
			handler := func(packet *rtp.Packet) {
				if needsDeadline {
					deadline.Reset(core.ConnDeadline)
				}
				audioTrack.WriteRTP(packet)
				c.Recv += len(packet.Payload)
			}
			c.audioSession.OnReadRTP = timekeeper(handler)
		}
	}

	<-deadline.C

	return nil
}

func (c *Client) Stop() error {
	if c.backchannel != nil {
		c.backchannel.Close()
	}
	if c.forwardAudio != nil {
		c.forwardAudio.Close()
	}

	if c.videoSession != nil && c.videoSession.Remote != nil {
		c.srtp.DelSession(c.videoSession)
	}
	if c.audioSession != nil && c.audioSession.Remote != nil {
		c.srtp.DelSession(c.audioSession)
	}

	return c.Connection.Stop()
}

func (c *Client) trackByKind(kind string) *core.Receiver {
	for _, receiver := range c.Receivers {
		if receiver.Codec.Kind() == kind {
			return receiver
		}
	}
	return nil
}

func (c *Client) startMJPEG() error {
	receiver := c.Receivers[0]

	for {
		b, err := c.hap.GetImage(1920, 1080)
		if err != nil {
			return err
		}

		c.Recv += len(b)

		packet := &rtp.Packet{
			Header:  rtp.Header{Timestamp: core.Now90000()},
			Payload: b,
		}
		receiver.WriteRTP(packet)
	}
}

func (c *Client) srtpEndpoint() *srtp.Endpoint {
	return &srtp.Endpoint{
		Addr:       c.hap.LocalIP(),
		Port:       uint16(c.srtp.Port()),
		MasterKey:  []byte(core.RandString(16, 0)),
		MasterSalt: []byte(core.RandString(14, 0)),
		SSRC:       rand.Uint32(),
	}
}

func timekeeper(handler core.HandlerFunc) core.HandlerFunc {
	const sampleRate = 16000
	const sampleSize = 480

	var send time.Duration
	var firstTime time.Time

	return func(packet *rtp.Packet) {
		now := time.Now()

		if send != 0 {
			elapsed := now.Sub(firstTime) * sampleRate / time.Second
			if send+sampleSize > elapsed {
				return // drop overflow frame
			}
		} else {
			firstTime = now
		}

		send += sampleSize

		packet.Timestamp = uint32(send)

		handler(packet)
	}
}
