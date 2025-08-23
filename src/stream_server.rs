use std::{
    sync::Mutex,
    thread::{self, JoinHandle},
};

use anyhow::{anyhow, Result};
use ashpd::desktop::{
    screencast::{CursorMode, Screencast, SourceType},
    PersistMode,
};
use gst::{glib, prelude::*, ClockTime, MessageView};
use gst_rtsp_server::prelude::*;
use gstreamer::{self as gst, glib::ControlFlow};
use gstreamer_rtsp_server as gst_rtsp_server;
use x11rb::connection::Connection;
use x11rb::protocol::randr::*;

use crate::config::DesktopCastConfig;

struct VideoSourceHelper;
impl VideoSourceHelper {
    async fn get_pipewire_stream_id() -> Result<u32> {
        let proxy = Screencast::new().await?;
        let session = proxy.create_session().await?;
        proxy
            .select_sources(
                &session,
                CursorMode::Hidden,
                SourceType::Monitor | SourceType::Window,
                false,
                None,
                PersistMode::DoNot,
            )
            .await?;

        let response = proxy.start(&session, None).await?.response()?;

        response.streams().iter().for_each(|stream| {
            println!("node id: {}", stream.pipe_wire_node_id());
            println!("size: {:?}", stream.size());
            println!("position: {:?}", stream.position());
        });
        let stream = response
            .streams()
            .iter()
            .next()
            .ok_or(anyhow!("No streams!"))?;
        Ok(stream.pipe_wire_node_id())
    }

    fn get_x11_options() -> Result<String> {
        let (conn, screen_num) = x11rb::connect(None).unwrap();
        let screen = &conn.setup().roots[screen_num];

        let monitors = conn.randr_get_monitors(screen.root, true)?;
        let monitors = monitors.reply()?;

        let primary_monitor = monitors
            .monitors
            .iter()
            .filter(|m| m.primary)
            .next()
            .ok_or_else(|| anyhow!("Failed to retrieve primary monitor!"))?;

        Ok(format!(
            "startx={} starty={} endx={} endy={} use-damage=0",
            primary_monitor.x,
            primary_monitor.y,
            primary_monitor
                .x
                .saturating_add(primary_monitor.width as i16 - 1),
            primary_monitor
                .y
                .saturating_add(primary_monitor.height as i16 - 1)
        ))
    }

    pub async fn get_gst_videosource_launch() -> Result<String> {
        // first try pipewire/xdg-portal,
        // then try x11 primary monitor
        // then fall back to x11 entire screen
        if let Ok(pipewire_id) = VideoSourceHelper::get_pipewire_stream_id().await {
            Ok(format!(
                "pipewiresrc keepalive-time=100 path={} ! retimestamp",
                pipewire_id
            ))
        } else if let Ok(ximagesrc_args) = VideoSourceHelper::get_x11_options() {
            Ok(format!("ximagesrc {}", ximagesrc_args))
        } else {
            Ok("ximagesrc use-damage=0".to_string())
        }
    }
}

struct AudioSourceHelper;
impl AudioSourceHelper {
    pub async fn get_gst_audiosource_launch() -> Result<String> {
        let device_monitor = gst::DeviceMonitor::new();
        let filter = gst::Caps::new_empty_simple("audio/x-raw");
        device_monitor.add_filter(Some("Audio/Source"), Some(&filter));
        let sound_devices = device_monitor.devices();
        let sound_monitor = sound_devices
            .iter()
            .filter(|dev| dev.name().starts_with("pulse")) // hard-coded to pulseaudio for now
            .filter(|dev| {
                if let Some(properties) = dev.properties() {
                    if properties.has_field("device.class") {
                        let device_class = properties.get::<String>("device.class");
                        if let Ok(device_class) = device_class {
                            return device_class == "monitor";
                        }
                    }
                }
                false
            })
            .next();

        let sound_monitor = sound_monitor
            .ok_or_else(|| anyhow!("No Sound monitor found! Can't forward audio output"))?;

        let pulse_device = sound_monitor.property::<String>("internal-name");

        Ok(format!("pulsesrc device={} ! retimestamp", pulse_device))
    }
}

pub struct StreamServer {
    main_loop: glib::MainLoop,
    server: gst_rtsp_server::RTSPServer,
    worker_thread: Option<JoinHandle<()>>,
}
impl StreamServer {
    pub fn new() -> Self {
        let main_loop = glib::MainLoop::new(None, false);
        let server = gst_rtsp_server::RTSPServer::new();
        server.set_backlog(1);

        Self {
            main_loop,
            server,
            worker_thread: None,
        }
    }

    pub async fn start(&mut self, config: &DesktopCastConfig) -> Result<()> {
        let nproc = num_cpus::get();

        let mounts = self
            .server
            .mount_points()
            .ok_or_else(|| anyhow!("Failed to register rtsp server endpoint"))?;

        let factory = gst_rtsp_server::RTSPMediaFactory::new();

        // construct pipeline
        let video_source = VideoSourceHelper::get_gst_videosource_launch().await?;
        let audio_source = AudioSourceHelper::get_gst_audiosource_launch().await?;

        let mut pipeline_str = "multiqueue name=outqueue".to_owned();
        // VIDEO
        pipeline_str += &format!(" {} ! queue max-size-time=250000000 leaky=2", video_source);
        if let Some(rescale_res) = &config.target_resolution {
            pipeline_str += &format!(
                " ! videoscale n-threads={} ! video/x-raw,width={},height={}",
                nproc, rescale_res.width, rescale_res.height
            );
        }
        pipeline_str += &format!(
            " ! videoconvert ! queue max-size-time=250000000 ! x264enc threads={} tune=zerolatency speed-preset=3 bframes=0 pass=qual quantizer=24 ! video/x-h264,profile=high ! outqueue. outqueue. ! rtph264pay name=pay0 pt=96",
            nproc
        );
        // AUDIO
        pipeline_str += &format!(" {} ! queue max-size-time=250000000 leaky=2 ! audioconvert ! audioresample ! queue max-size-time=250000000 ! vorbisenc ! outqueue. outqueue. ! rtpvorbispay name=pay1 pt=97", audio_source);

        factory.set_launch(&format!("( {} )", pipeline_str));
        factory.set_shared(true);
        factory.set_latency(0);
        factory.set_retransmission_time(ClockTime::from_mseconds(100));
        factory.set_stop_on_disconnect(true);

        factory.connect_media_constructed({
            let main_loop = self.main_loop.clone();
            // do not judge me. The add_watch() returning a guard is super dumb and annoying here
            let bus_watches = Mutex::new(Vec::new());
            move |_, media| {
                let bus = media.element().bus().unwrap();
                let bus_watch = bus
                    .add_watch({
                        let main_loop = main_loop.clone();
                        move |_, msg| {
                            if let MessageView::Error(err) = msg.view() {
                                eprintln!("Pipeline failed:\n{:?}", err);
                                main_loop.quit();
                                ControlFlow::Break
                            } else {
                                ControlFlow::Continue
                            }
                        }
                    })
                    .unwrap();
                bus_watches.lock().unwrap().push(bus_watch);
            }
        });

        mounts.add_factory("/", factory);

        let _id = self.server.attach(None)?;
        self.worker_thread = Some(thread::spawn({
            let main_loop = self.main_loop.clone();
            move || {
                main_loop.run();
            }
        }));

        Ok(())
    }

    pub fn run(&mut self) -> Result<()> {
        let worker_thread = self
            .worker_thread
            .take()
            .expect("Either run() was called twice, or run() was called before start()");
        worker_thread
            .join()
            .map_err(|e| anyhow!("StreamServer crashed: {:?}", e))?;
        Ok(())
    }
}
