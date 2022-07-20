#! /usr/bin/env python3

import json
import requests
import time
from datetime import datetime
import psycopg2
import telebot
import sqlite3
import os

bot_token = open("bot_token.txt", "r").read()
bot = telebot.TeleBot(bot_token)
db_file = "database.db"

second_to_nanoseconds = 1_000_000_000
minute_to_nanoseconds = 60 * second_to_nanoseconds
hour_to_nanoseconds = 60 * minute_to_nanoseconds


def create_db():
    if not os.path.exists(db_file):
        with sqlite3.connect(db_file) as conn:
            conn.executescript(
                """
                         CREATE TABLE users(
                         user_id text primary key
                         ); 
                         """
            )
            conn.executescript(
                """
                         CREATE TABLE test_users(
                         user_id text primary key
                         ); 
                         """
            )
            conn.executescript(
                """
                        CREATE TABLE variables(
                        name text primary key,
                        value text
                        );
                         """
            )
            conn.executescript(
                f"""
                         insert into variables(name, value)
                         values
                         ('time', '{int(int(time.time()) * 1_000_000_000)}'),
                         ('test_time', '{int(int(time.time()) * 1_000_000_000)}'),
                         ('offset', '{0}');
                         """
            )


def get_users_id(users):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
                   select * from {users}
                   """
        )
        return cursor.fetchall()


def get_users():
    return get_users_id("users")


def get_test_users():
    return get_users_id("test_users")


def get_variable(variable):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
                   select name, value from variables where name = '{variable}'
                   """
        )
        (text, value) = (cursor.fetchall())[0]
        return value


def get_max_time():
    return get_variable("time")


def get_max_test_time():
    return get_variable("test_time")


def get_offset():
    return get_variable("offset")


def update_variable(variable, value):
    with sqlite3.connect(db_file) as conn:
        conn.executescript(
            f"""
                       update variables
                       set value='{value}'
                       where name='{variable}'
                       """
        )


def update_max_time(new_max_time):
    update_variable("time", new_max_time)


def update_max_test_time(new_max_test_time):
    update_variable("test_time", new_max_test_time)


def update_offset(new_offset):
    update_variable("offset", new_offset)


def send_message_to_users(message):
    users = get_users()
    for user in users:
        bot.send_message(int(user[0]), message, parse_mode='HTML')


def send_message_to_test_users(message):
    test_users = get_test_users()
    for user in test_users:
        bot.send_message(int(user[0]), message, parse_mode='HTML')


def get_network(net):
    if net == "testnet":
        return psycopg2.connect(
            host="testnet.db.explorer.indexer.near.dev",
            database="testnet_explorer",
            user="public_readonly",
            password="nearprotocol",
        )
    elif net == "mainnet":
        return psycopg2.connect(
            host="mainnet.db.explorer.indexer.near.dev",
            database="mainnet_explorer",
            user="public_readonly",
            password="nearprotocol",
        )
    else:
        raise "Wrong network type!"


def get_period(period):
    if period == "1m":
        return int(time.time() * second_to_nanoseconds - minute_to_nanoseconds)
    elif period == "5m":
        return int(time.time() * second_to_nanoseconds - minute_to_nanoseconds * 5)
    elif period == "15m":
        return int(time.time() * second_to_nanoseconds - minute_to_nanoseconds * 15)
    elif period == "30m":
        return int(time.time() * second_to_nanoseconds - minute_to_nanoseconds * 30)
    elif period == "1h":
        return int(time.time() * second_to_nanoseconds - hour_to_nanoseconds)
    elif period == "2h":
        return int(time.time() * second_to_nanoseconds - hour_to_nanoseconds * 2)
    elif period == "3h":
        return int(time.time() * second_to_nanoseconds - hour_to_nanoseconds * 3)
    elif period == "6h":
        return int(time.time() * second_to_nanoseconds - hour_to_nanoseconds * 6)
    elif period == "12h":
        return int(time.time() * second_to_nanoseconds - hour_to_nanoseconds * 12)
    elif period == "24h" or period == "1d":
        return int(time.time() * second_to_nanoseconds - hour_to_nanoseconds * 24)
    else:
        raise "Wrong period type!"


def check_args(args):
    return (
        "args_json" in args.keys()
        and "method_name" in args.keys()
        and args["method_name"] == "nft_mint"
    )

def get_collection(contract, net):
    pload = {"jsonrpc": "2.0", "id": "dontcare", "method": "query", "params": {"request_type": "call_function",  "finality": "final", "account_id": contract, "method_name": "nft_metadata", "args_base64": "e30="}}
    if net == "testnet":
        r = requests.post("https://rpc.testnet.near.org", json=pload)
    else:
        r = requests.post("https://rpc.mainnet.near.org", json=pload)
    result = json.loads(bytearray(r.json()["result"]["result"]).decode("utf-8"))
    return result["name"]


def cmp(value):
    return value[1]


def start(message):
    args = message.text.split()
    if len(args) != 1:
        bot.send_message(message.chat.id, "Try again!")
    else:
        bot.send_message(message.chat.id, "Hey!")


def help(message):
    args = message.text.split()
    if len(args) != 1:
        bot.send_message(message.chat.id, "Try again!")
    else:
        bot.send_message(
            message.chat.id,
            "<u><b>All available commands:</b></u>\n"
            "/get_feed <em>network</em> <em>period</em>: Display feed for a given period \n"
            # "network = mainnet | testnet\nperiod = 1m | 5m | 15m | 30m | 1h | 6h | 12h | 1d\n"
            "/notify_on <em>network</em>: Turn on notifications for newcomer mints\n"
            "/notify_off <em>network</em>: Turn off notifications for newcomer mints\n"
            "/help: Display help",
            parse_mode="HTML",
        )


def notify_on(message):
    args = message.text.split()
    if len(args) == 1 or (len(args) == 2 and args[1] == "mainnet"):
        with sqlite3.connect(db_file) as conn:
            conn.executescript(
                f"""
                    insert or ignore into users(user_id)
                    values
                    ('{message.chat.id}')
                """
            )
        bot.send_message(
            message.chat.id,
            "Notifications(mainnet) turned <b>on</b>",
            parse_mode="HTML",
        )
    elif len(args) == 2 and args[1] == "testnet":
        with sqlite3.connect(db_file) as conn:
            conn.executescript(
                f"""
                    insert or ignore into test_users(user_id)
                    values
                    ('{message.chat.id}')
                """
            )
        bot.send_message(
            message.chat.id,
            "Notifications(testnet) turned <b>on</b>",
            parse_mode="HTML",
        )
    else:
        bot.send_message(message.chat.id, "Try again!")


def notify_off(message):
    args = message.text.split()
    if len(args) == 1 or (len(args) == 2 and args[1] == "mainnet"):
        with sqlite3.connect(db_file) as conn:
            conn.executescript(
                f"""
                    delete from users
                    where user_id = '{message.chat.id}'
                """
            )
        bot.send_message(
            message.chat.id, "Notifications(mainnet) turned <b>off</b>", parse_mode="HTML"
        )
    elif len(args) == 2 and args[1] == "testnet":
        with sqlite3.connect(db_file) as conn:
            conn.executescript(
                f"""
                    delete from test_users
                    where user_id = '{message.chat.id}'
                """
            )
        bot.send_message(
            message.chat.id, "Notifications(testnet) turned <b>off</b>", parse_mode="HTML"
        )
    else:
        bot.send_message(message.chat.id, "Try again!")


def get_feed(message):
    message_args = message.text.split()[1:]
    if len(message_args) != 2:
        bot.send_message(message.chat.id, "Try again!(Wrong number of arguments)")
        return

    try:
        conn = get_network(message_args[0])
    except:
        bot.send_message(message.chat.id, "Try again!(Wrong network type)")
        return
    try:
        timestamp = get_period(message_args[1])
    except:
        bot.send_message(message.chat.id, "Try again!(Wrong period type)")
        return

    cur = conn.cursor()
    cur.execute(
        f"""
              select 
                  t.emitted_for_receipt_id as id,
                  t.emitted_at_block_timestamp as time,
                  t.emitted_by_contract_account_id,
                  t.event_kind as event,
                  w.args as args
              from 
                  assets__non_fungible_token_events t,
                  action_receipt_actions w
              where
                  t.emitted_at_block_timestamp > {timestamp} and 
                  t.event_kind = 'MINT' and
                  t.emitted_for_receipt_id = w.receipt_id
              """
    )

    mints = cur.fetchall()
    number_of_mints = dict()
    for (receipt_id, _, contract, _, args) in mints:
        if not check_args(args):
            continue

        number_of_mints[contract] = number_of_mints.setdefault(contract, 0) + 1

    feed = list(number_of_mints.items())
    feed.sort(key=cmp, reverse=True)
    feed = feed[:20]

    msg = ""
    for (contract, value) in feed:
        collection = get_collection(contract, message_args[0])
        msg += f"Collection: {collection}\nMints: {value}\n================\n"
    if len(feed) == 0:
        msg = "No NFT's were minted."

    bot.send_message(message.chat.id, msg)


def process_new_update(update):
    if update.message == None or update.message.text == None:
        return
    args = update.message.text.split()
    if args[0] == "/start":
        start(update.message)
    elif args[0] == "/help":
        help(update.message)
    elif args[0] == "/notify_on":
        notify_on(update.message)
    elif args[0] == "/notify_off":
        notify_off(update.message)
    elif args[0] == "/get_feed":
        get_feed(update.message)


def process_new_updates(updates):
    for update in updates:
        process_new_update(update)


def get_updates():
    offset = int(get_offset())
    updates = bot.get_updates(offset)
    for update in updates:
        offset = max(offset, update.update_id + 1)
    update_offset(offset)
    process_new_updates(updates)


def get_new_transactions(net):
    if net == "mainnet":
        cur_max_time = get_max_time()
    else:
        cur_max_time = get_max_test_time()
    conn = get_network(net)
    cur = conn.cursor()
    cur.execute(
        f"""
              select 
                  t.emitted_for_receipt_id as id,
                  t.emitted_at_block_timestamp as time,
                  t.emitted_by_contract_account_id,
                  t.token_new_owner_account_id as owner,
                  t.event_kind as event,
                  w.args as args
              from 
                  assets__non_fungible_token_events t,
                  action_receipt_actions w
              where
                  t.emitted_at_block_timestamp > {cur_max_time} and 
                  t.event_kind = 'MINT' and
                  t.emitted_for_receipt_id = w.receipt_id
              """
    )

    new_max_time = 0
    mints = cur.fetchall()
    for (id, time, contract, owner_id, _, args) in mints:
        new_max_time = max(new_max_time, time)
        if not check_args(args):
            continue

        collection = get_collection(contract, net)
        msg = f"NFT MINTED!\n<b>{owner_id}</b> just minted NFT from <b>{collection}</b> collection on {net}"
        if net == "mainnet":
            send_message_to_users(msg)
        else:
            send_message_to_test_users(msg)

    if new_max_time > int(cur_max_time):
        if net == "mainnet":
            update_max_time(new_max_time)
        else:
            update_max_test_time(new_max_time)


if __name__ == "__main__":
    create_db()
    get_updates()
    get_new_transactions("mainnet")
    get_new_transactions("testnet")
