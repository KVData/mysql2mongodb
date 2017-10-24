#[macro_use]
extern crate mysql;
#[macro_use(doc, bson)]
extern crate bson;
extern crate mongodb;
extern crate config;
extern crate serde;
#[macro_use]
extern crate serde_derive;

mod settings;

use std::time::SystemTime;

use mysql::*;
use bson::{Bson, Document};
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;

use settings::Settings;

#[derive(Debug)]
struct User {
    id: String,
    name: String,
    province: String,
    register_time: String,
    level_name: String,
    is_mobile: String,
}

impl User {
    fn to_bson(&self) -> Document {
        let doc = doc! {
            "id" => &self.id,
            "name" => &self.name,
            "province" => &self.province,
            "register_time" => &self.register_time,
            "level_name" => &self.level_name,
            "is_mobile" => &self.is_mobile,
        };
        doc
    }
}

#[derive(Debug)]
struct Product {
    id: String,
    name: String,
    color: String,
    size: String,
}

impl Product {
    fn to_bson(&self) -> Document {
        let doc = doc! {
            "id" => &self.id,
            "name" => &self.name,
            "color" => &self.color,
            "size" => &self.size,
        };
        doc
    }
}

#[derive(Debug)]
struct Comment {
    user: User,
    product: Product,
    content: String,
    date: String,
    reply_count: u32,
    score: u32,
    status: String,
    title: String,
    days: u32,
    tags: String,
}

impl Comment {
    fn to_bson(&self) -> Document {
        let doc = doc! {
            "_id" => &self.user.id,
            "user" => self.user.to_bson(),
            "product" => self.product.to_bson(),
            "content" => &self.content,
            "date" => &self.date,
            "reply_count" => self.reply_count,
            "score" => self.score,
            "status" => &self.status,
            "title" => &self.title,
            "days" => self.days,
            "tags" => &self.tags,
        };
        doc
    }
}

#[derive(Debug)]
struct Goods {
    id: String,
    name: String,
    comment_num: u32,
    shop_name: String,
    link: String,
    comment_version: String,
    score_1_count: u32,
    score_2_count: u32,
    score_3_count: u32,
    score_4_count: u32,
    score_5_count: u32,
    price: f32,
    comments: Vec<Comment>,
}

impl Goods {
    fn to_bson(&self) -> Document {
        let doc = doc! {
            "_id" => &self.id,
            "name" => &self.name,
            "comment_num" => self.comment_num,
            "shop_name" => &self.shop_name,
            "link" => &self.link,
            "comment_version" => &self.comment_version,
            "score_1_count" => self.score_1_count,
            "score_2_count" => self.score_2_count,
            "score_3_count" => self.score_3_count,
            "score_4_count" => self.score_4_count,
            "score_5_count" => self.score_5_count,
            "price" => self.price,
            "comments" => Bson::Array(self.comments.iter().map(|comment| {
                bson!(comment.to_bson())
            }).collect()),
        };
        doc
    }
}

fn get_goods_count(pool: &mysql::Pool) -> u32 {
    let count: Option<u32> = pool.first_exec(r"SELECT count(*) FROM jd_goods", ())
        .map(|result| {
            result.map(|x| x.unwrap()).map(|mut row| {
                from_value::<u32>(row.pop().unwrap())
            })
        }).unwrap();

    return count.unwrap();
}

fn get_goods_by_page(pool: &mysql::Pool, current_size: u32, page_size: u32) -> Vec<Goods> {
    let selected_goods: Vec<Goods> = pool.prep_exec(r"SELECT * FROM jd_goods LIMIT :current_size, :page_size", params! {
        "current_size" => current_size,
        "page_size" => page_size,
    }).map(|result| {
        result.map(|x| x.unwrap()).map(|mut row| {
            let id: String = row.take("ID").unwrap();
            Goods {
                id: id.to_string(),
                name: row.take("name").unwrap(),
                comment_num: row.take("comment_num").unwrap(),
                shop_name: row.take("shop_name").unwrap(),
                link: row.take("link").unwrap(),
                comment_version: row.take("commentVersion").unwrap(),
                score_1_count: row.take("score1count").unwrap(),
                score_2_count: row.take("score2count").unwrap(),
                score_3_count: row.take("score3count").unwrap(),
                score_4_count: row.take("score4count").unwrap(),
                score_5_count: row.take("score5count").unwrap(),
                price: row.take("price").unwrap(),
                comments: get_comments_by_goods(pool, &id),
            }
        }).collect()
    }).unwrap();

    return selected_goods;
}

fn get_comments_by_goods(pool: &mysql::Pool, good_id: &str) -> Vec<Comment> {
    let selected_comments: Vec<Comment> = pool.prep_exec(r"SELECT * FROM jd_comment WHERE good_ID = :good_id", params! {
        "good_id" => good_id,
    }).map(|result| {
        result.map(|x| x.unwrap()).map(|mut row| {
            Comment {
                user: User {
                    id: row.take("user_ID").unwrap(),
                    name: row.take("user_name").unwrap(),
                    province: row.take("userProvince").unwrap(),
                    register_time: row.take("userRegisterTime").unwrap(),
                    level_name: row.take("userLevelName").unwrap(),
                    is_mobile: row.take("isMobile").unwrap(),
                },
                product: Product {
                    id: row.take("good_ID").unwrap(),
                    name: row.take("good_name").unwrap(),
                    color: row.take("productColor").unwrap(),
                    size: row.take("productSize").unwrap(),
                },
                content: row.take("content").unwrap(),
                date: row.take("date").unwrap(),
                reply_count: row.take("replyCount").unwrap(),
                score: row.take("score").unwrap(),
                status: row.take("status").unwrap(),
                title: row.take("title").unwrap(),
                days: row.take("days").unwrap(),
                tags: row.take("tags").unwrap(),
            }
        }).collect()
    }).unwrap();
    return selected_comments;
}

fn get_comments_count(pool: &mysql::Pool) -> u32 {
    let count: Option<u32> = pool.first_exec(r"SELECT count(*) FROM jd_comment", ())
        .map(|result| {
            result.map(|x| x.unwrap()).map(|mut row| {
                from_value::<u32>(row.pop().unwrap())
            })
        }).unwrap();

    return count.unwrap();
}

fn get_comments_by_page(pool: &mysql::Pool, current_size: u32, page_size: u32) -> Vec<Comment> {
    let selected_comments: Vec<Comment> = pool.prep_exec(r"SELECT * FROM jd_comment LIMIT :current_size, :page_size", params! {
        "current_size" => current_size,
        "page_size" => page_size,
    }).map(|result| {
        result.map(|x| x.unwrap()).map(|mut row| {
            Comment {
                user: User {
                    id: row.take("user_ID").unwrap(),
                    name: row.take("user_name").unwrap(),
                    province: row.take("userProvince").unwrap(),
                    register_time: row.take("userRegisterTime").unwrap(),
                    level_name: row.take("userLevelName").unwrap(),
                    is_mobile: row.take("isMobile").unwrap(),
                },
                product: Product {
                    id: row.take("good_ID").unwrap(),
                    name: row.take("good_name").unwrap(),
                    color: row.take("productColor").unwrap(),
                    size: row.take("productSize").unwrap(),
                },
                content: row.take("content").unwrap(),
                date: row.take("date").unwrap(),
                reply_count: row.take("replyCount").unwrap(),
                score: row.take("score").unwrap(),
                status: row.take("status").unwrap(),
                title: row.take("title").unwrap(),
                days: row.take("days").unwrap(),
                tags: row.take("tags").unwrap(),
            }
        }).collect()
    }).unwrap();
    return selected_comments;
}

fn main() {
    let settings = Settings::new().unwrap();

    let pool = mysql::Pool::new(settings.mysql.url)
        .expect("Failed to connect to mysql database.");

    let client = Client::connect(&settings.mongodb.host, settings.mongodb.port)
        .expect("Failed to initialize standalone client.");
    let collection = client.db(&settings.mongodb.db).collection(&settings.mongodb.collection);

    // let goods_count = get_goods_count(&pool);
    // println!("There are {} goods in total.", goods_count);

    // let mut current_size = 0u32;

    // let now = SystemTime::now();

    // loop {
    //     let goods = get_goods_by_page(&pool, current_size, settings.app.page_size);

    //     for x in goods.iter() {
    //         collection.insert_one(x.to_bson(), None)
    //             .expect("Failed to insert document.");
    //     }

    //     println!("Process {} goods.", goods.len());
    //     match now.elapsed() {
    //         Ok(elapsed) => {
    //             println!("{}", elapsed.as_secs());
    //         }
    //         Err(e) => {
    //             println!("Error: {:?}", e);
    //         }
    //     }

    //     current_size += settings.app.page_size;

    //     if current_size >= goods_count {
    //         break;
    //     }
    // }

    let page_size = settings.app.page_size;
    let comments_count = get_comments_count(&pool);
    println!("There are {} comments in total.", comments_count);

    let mut current_size = 800000u32;

    let now = SystemTime::now();

    loop {
        let comments = get_comments_by_page(&pool, current_size, page_size);

        for x in comments.iter() {
            collection.insert_one(x.to_bson(), None)
                .expect("Failed to insert document.");
        }

        println!("Process {} comments.", comments.len());
        match now.elapsed() {
            Ok(elapsed) => {
                println!("{}", elapsed.as_secs());
            }
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }

        current_size += page_size;

        if current_size > comments_count {
            break;
        }
    }
}
