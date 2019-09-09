use mqtt;
use mqtt::packet::*;
use serde_json::{self, Result, Value};
use mqtt::{Decodable, Encodable, QualityOfService};
use mqtt::{TopicFilter, TopicName};
use std::env;
use std::io::{self, Write};
use serde_derive::{Serialize, Deserialize};
use std::io::{Error, ErrorKind};
use log::{info, warn, error, trace};
use std::thread;
use std::time::{Duration, Instant};

use ::futures::Future;
use mysql;
use std::sync::{Arc, Mutex, Condvar, RwLock};
use crossbeam_channel::{bounded, tick, Sender, Receiver, select};
use std::collections::{HashMap, BTreeMap};
use std::cell::RefCell;
use std::rc::Rc;

use crate::room::*;
use crate::msg::*;
use std::process::Command;

const TEAM_SIZE: u16 = 1;
const MATCH_SIZE: usize = 2;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateRoomData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CloseRoomData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InviteRoomData {
    pub rid: String,
    pub cid: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JoinRoomData {
    pub rid: String,
    pub cid: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserLoginData {
    pub u: User,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserNGHeroData {
    pub id: String,
    pub hero: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserLogoutData {
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StartQueueData {
    pub room: String,
    pub action: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CancelQueueData {
    pub room: String,
    pub action: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PreStartData {
    pub room: String,
    pub id: String,
    pub accept: bool,
}

pub enum RoomEventData {
    Reset(),
    Login(UserLoginData),
    Logout(UserLogoutData),
    Create(CreateRoomData),
    Close(CloseRoomData),
    ChooseNGHero(UserNGHeroData),
    Invite(InviteRoomData),
    Join(JoinRoomData),
    StartQueue(StartQueueData),
    CancelQueue(CancelQueueData),
    PreStart(PreStartData),
}

// Prints the elapsed time.
fn show(dur: Duration) {
    println!(
        "Elapsed: {}.{:03} sec",
        dur.as_secs(),
        dur.subsec_nanos() / 1_000_000
    );
}

fn SendGameList(game: &FightGame, msgtx: Sender<MqttMsg>, conn: &mut mysql::PooledConn) {
    pub struct ListCell {
        pub id: String,
        pub name: String,
        pub hero: String,
    }
    pub struct GameCell {
        pub game: u64,  
        pub team1: Vec<ListCell>,
        pub team2: Vec<ListCell>,
    }

    for r in &game.room_names {
        let sql = format!("select userid,name from user where userid='{}';", r);
        println!("sql: {}", sql);
        let qres = conn.query(sql.clone()).unwrap();
        let mut userid: String = "".to_owned();
        let mut name: String = "".to_owned();

        let mut count = 0;
        for row in qres {
            count += 1;
            let a = row.unwrap().clone();
            userid = mysql::from_value(a.get("userid").unwrap());
            name = mysql::from_value(a.get("name").unwrap());
            break;
        }

    }
}

pub fn init(msgtx: Sender<MqttMsg>, pool: mysql::Pool) -> Sender<RoomEventData> {
    let (tx, rx):(Sender<RoomEventData>, Receiver<RoomEventData>) = bounded(1000);
    let start = Instant::now();
    let update200ms = tick(Duration::from_millis(200));
    let update100ms = tick(Duration::from_millis(100));
    
    thread::spawn(move || {
        let mut conn = pool.get_conn().unwrap();
        let mut TotalRoom: BTreeMap<String, Rc<RefCell<RoomData>>> = BTreeMap::new();
        let mut QueueRoom: BTreeMap<String, Rc<RefCell<RoomData>>> = BTreeMap::new();
        let mut ReadyGroups: Vec<Rc<RefCell<FightGroup>>> = vec![];
        let mut PreStartGroups: Vec<FightGame> = vec![];
        let mut GameingGroups: Vec<FightGame> = vec![];
        let mut RoomMap: BTreeMap<String, Rc<RefCell<RoomData>>> = BTreeMap::new();
        let mut TotalUsers: Vec<User> = vec![];
        let mut TotalUserStatus: BTreeMap<String, UserStatus> = BTreeMap::new();
        let mut roomCount: u32 = 0;
        let mut game_port: u16 = 7777;
        let mut game_id: u64 = 0;
        loop {
            select! {
                recv(update200ms) -> _ => {
                    //show(start.elapsed());
                    if QueueRoom.len() >= MATCH_SIZE {
                        let mut g: FightGroup = Default::default();
                        let mut tq: Vec<Rc<RefCell<RoomData>>> = vec![];
                        tq = QueueRoom.iter().map(|x|Rc::clone(x.1)).collect();
                        tq.sort_by_key(|x| x.borrow().avg_rk);
                        for (k, v) in &mut QueueRoom {
                            if v.borrow().ready == 0 &&
                                v.borrow().users.len() as u16 + g.user_count <= TEAM_SIZE {
                                g.add_room(Rc::clone(&v));
                            }
                            if g.user_count == TEAM_SIZE {
                                g.prestart();
                                ReadyGroups.push(Rc::new(RefCell::new(g.clone())));
                                g = Default::default();
                            }
                        }
                    }
                    if ReadyGroups.len() >= MATCH_SIZE {
                        let mut fg: FightGame = Default::default();
                        for rg in &mut ReadyGroups {
                            if rg.borrow().game_status == 0 && fg.teams.len() < MATCH_SIZE {
                                fg.teams.push(Rc::clone(rg));
                            }
                            if fg.teams.len() == MATCH_SIZE {
                                for g in &mut fg.teams {
                                    let gstatus = g.borrow().game_status;
                                    if gstatus == 0 {
                                        for r in &mut g.borrow_mut().rooms {
                                            r.borrow_mut().ready = 2;
                                        }
                                    }
                                    g.borrow_mut().game_status = 1;
                                }
                                fg.update_names();
                                for r in &fg.room_names {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/prestart", r), msg: r#"{"msg":"prestart"}"#.to_string()});
                                }
                                PreStartGroups.push(fg.clone());
                                fg = Default::default();
                            }
                        }
                    }
                    // update prestart groups
                    let mut i = 0;
                    while i != PreStartGroups.len() {
                        match PreStartGroups[i].check_prestart() {
                            PrestartStatus::Ready => {
                                let mut start_group = PreStartGroups.remove(i);
                                game_port += 1;
                                game_id += 1;
                                if game_port > 65500 {
                                    game_port = 7777;
                                }
                                if game_id > u64::max_value()-10 {
                                    game_id = 0;
                                }
                                start_group.ready();
                                start_group.update_names();
                                for r in &start_group.room_names {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/start", r), 
                                        msg: format!(r#"{{"room":"{}","msg":"start","server":"59.126.81.58:{}","game":{}}}"#, 
                                            r, game_port, game_id)});
                                }
                                GameingGroups.push(start_group.clone());
                                println!("GameingGroups {:#?}", GameingGroups);
                                println!("game_port: {}", game_port);
                                Command::new("/home/damody/LinuxNoEditor/CF1/Binaries/Linux/CF1Server")
                                        .arg(format!("-Port={}", game_port))
                                        .spawn()
                                        .expect("sh command failed to start");
                                std::thread::sleep_ms(10000);
                                SendGameList(&start_group, msgtx.clone(), &mut conn);
                            },
                            PrestartStatus::Cancel => {
                                let mut start_group = PreStartGroups.remove(i);
                                start_group.update_names();
                                for r in &start_group.room_names {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/prestart", r), 
                                        msg: format!(r#"{{"msg":"stop queue"}}"#)});
                                }
                            },
                            PrestartStatus::Wait => {
                                i += 1;
                            }
                        }
                    }
                }
                recv(rx) -> d => {
                    if let Ok(d) = d {
                        match d {
                            RoomEventData::ChooseNGHero(x) => {
                                for u in &mut TotalUsers {
                                    if u.id == x.id {
                                        u.hero = x.hero;
                                        break;
                                    }
                                }
                            },
                            RoomEventData::Invite(x) => {
                                let mut hasUser = false;
                                for u in &TotalUsers {
                                    if u.id == x.cid {
                                        hasUser = true;
                                        break;
                                    }
                                }
                                if hasUser {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/invite", x.cid.clone()), 
                                        msg: format!(r#"{{"rid":"{}","cid":"{}"}}"#, x.rid.clone(), x.cid.clone())});
                                }
                                println!("Invite {:#?}", x);
                            },
                            RoomEventData::Join(x) => {
                                let mut tu:User = Default::default();
                                let mut hasUser = false;
                                for u in &TotalUsers {
                                    if u.id == x.cid {
                                        tu = u.clone();
                                        hasUser = true;
                                        break;
                                    }
                                }
                                let mut hasRoom = false;
                                if hasUser {
                                    let r = TotalRoom.get(&x.rid);
                                    match r {
                                        Some(r) => {
                                            r.borrow_mut().users.push(tu);
                                            println!("Join {:#?}", r);
                                            hasRoom = true;
                                        },
                                        _ => {}
                                    }
                                }
                                if hasRoom && hasUser {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/invite", x.cid.clone()), 
                                        msg: format!(r#"{{"rid":"{}","cid":"{}","accept":true}}"#, x.rid.clone(), x.cid.clone())});
                                }
                                else {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/invite", x.cid.clone()), 
                                        msg: format!(r#"{{"rid":"{}","cid":"{}","accept":false}}"#, x.rid.clone(), x.cid.clone())});
                                }
                            },
                            RoomEventData::Reset() => {
                                TotalRoom.clear();
                                QueueRoom.clear();
                                ReadyGroups.clear();
                                PreStartGroups.clear();
                                GameingGroups.clear();
                                RoomMap.clear();
                                TotalUsers.clear();
                                roomCount = 0;
                            },
                            RoomEventData::PreStart(x) => {
                                for r in &mut ReadyGroups {
                                    let mut rr = r.borrow_mut();
                                    if rr.check_has_room(&x.room) {
                                        if x.accept == true {
                                            rr.user_ready(&x.id);
                                        } else {
                                            rr.user_cancel(&x.id);
                                        }
                                        info!("PreStart: {}", x.id);
                                        break;
                                    }
                                }
                                //info!("ReadyGroups: {:#?}", ReadyGroups);
                            },
                            RoomEventData::StartQueue(x) => {
                                let mut success = false;
                                let mut hasRoom = false;
                                let r1 = QueueRoom.get(&x.room);
                                match r1 {
                                    Some(x) => {
                                        hasRoom = true;
                                    },
                                    _ => {}
                                }
                                if !hasRoom {
                                    let r = TotalRoom.get(&x.room);
                                    match r {
                                        Some(y) => {
                                            QueueRoom.insert(
                                                x.room.clone(),
                                                Rc::clone(y)
                                            );
                                            success = true;
                                        },
                                        _ => {}
                                    }
                                }
                                if success {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/start_queue", x.room.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)});
                                } else {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/start_queue", x.room.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)});
                                }
                                //info!("QueueRoom: {:#?}", QueueRoom);
                            },
                            RoomEventData::CancelQueue(x) => {
                                let mut success = false;
                                let data = QueueRoom.remove(&x.room);
                                match data {
                                    Some(x) => {
                                        success = true;
                                    },
                                    _ => {}
                                }
                                if success {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/cancel_queue", x.room.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)});
                                } else {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/cancel_queue", x.room.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)});
                                }
                            },
                            RoomEventData::Login(x) => {
                                let mut success = false;
                                if !TotalUsers.contains(&x.u) {
                                    TotalUsers.push(x.u.clone());
                                    success = true;
                                }
                                if success {
                                    msgtx.send(MqttMsg{topic:format!("member/{}/res/login", x.u.id.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)});
                                } else {
                                    msgtx.send(MqttMsg{topic:format!("member/{}/res/login", x.u.id.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)});
                                }
                            },
                            RoomEventData::Logout(x) => {
                                let mut success = false;
                                for i in 0..TotalUsers.len() {
                                    if TotalUsers[i].id == x.id {
                                        TotalUsers.remove(i);
                                        success = true;
                                        break;
                                    }
                                }
                                if success {
                                    msgtx.send(MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)});
                                } else {
                                    msgtx.send(MqttMsg{topic:format!("member/{}/res/logout", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)});
                                }
                            },
                            RoomEventData::Create(x) => {
                                let mut success = false;
                                if !RoomMap.contains_key(&x.id) {
                                    roomCount += 1;
                                    
                                    let mut new_room = RoomData {
                                        rid: roomCount,
                                        users: vec![],
                                        master: x.id.clone(),
                                        avg_ng: 0,
                                        avg_rk: 0,
                                        ready: 0,
                                    };
                                    for i in 0..TotalUsers.len() {
                                        if TotalUsers[i].id == x.id {
                                            new_room.add_user(&TotalUsers[i]);
                                            let r = Rc::new(RefCell::new(new_room));
                                            RoomMap.insert(
                                                x.id.clone(),
                                                Rc::clone(&r),
                                            );
                                            TotalRoom.insert(
                                                x.id.clone(),
                                                r,
                                            );
                                            success = true;
                                            break;
                                        }
                                    }
                                }
                                if success {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/create", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)});
                                } else {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/create", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)});
                                }
                            },
                            RoomEventData::Close(x) => {
                                let mut success = false;
                                if let Some(y) =  RoomMap.remove(&x.id) {
                                    let data = TotalRoom.remove(&x.id);
                                    match data {
                                        Some(_) => {
                                            QueueRoom.remove(&x.id);
                                            success = true;
                                        },
                                        _ => {}
                                    }
                                }
                                if success {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/cancel_queue", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"ok"}}"#)});
                                } else {
                                    msgtx.send(MqttMsg{topic:format!("room/{}/res/cancel_queue", x.id.clone()), 
                                        msg: format!(r#"{{"msg":"fail"}}"#)});
                                }
                            },
                        }
                    }
                }
            }
        }
    });
    tx
}

pub fn create(stream: &mut std::net::TcpStream, id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CreateRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Create(CreateRoomData{id: data.id.clone()}));
    Ok(())
}

pub fn close(stream: &mut std::net::TcpStream, id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CloseRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Close(CloseRoomData{id: data.id.clone()}));
    Ok(())
}

pub fn start_queue(stream: &mut std::net::TcpStream, id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: StartQueueData = serde_json::from_value(v)?;
    sender.send(RoomEventData::StartQueue(StartQueueData{room: data.room.clone(), action: data.action.clone()}));
    Ok(())
}

pub fn cancel_queue(stream: &mut std::net::TcpStream, id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: CancelQueueData = serde_json::from_value(v)?;
    sender.send(RoomEventData::CancelQueue(data));
    Ok(())
}

pub fn prestart(stream: &mut std::net::TcpStream, id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: PreStartData = serde_json::from_value(v)?;
    sender.send(RoomEventData::PreStart(data));
    Ok(())
}

pub fn join(stream: &mut std::net::TcpStream, id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: JoinRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Join(data));
    Ok(())
}

pub fn choose_ng_hero(stream: &mut std::net::TcpStream, id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: UserNGHeroData = serde_json::from_value(v)?;
    sender.send(RoomEventData::ChooseNGHero(data));
    Ok(())
}

pub fn invite(stream: &mut std::net::TcpStream, id: String, v: Value, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: InviteRoomData = serde_json::from_value(v)?;
    sender.send(RoomEventData::Invite(data));
    Ok(())
}
