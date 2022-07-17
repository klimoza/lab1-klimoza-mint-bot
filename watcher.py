#! /usr/bin/env python3

from datetime import datetime
import time
import psycopg2
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

bot_token = open("bot_token.txt", "r").read()

second_to_nanoseconds = 1_000_000_000
minute_to_nanoseconds = 60 * second_to_nanoseconds
hour_to_nanoseconds = 60 * minute_to_nanoseconds


def cmp(val):
    return val[1]


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Hey!")


async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="get_feed network period\nnetwork = mainnet | testnet\nperiod = 1m | 5m | 15m | 30m | 1h | 6h | 12h | 1d",
    )


async def get_feed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message == None:
        return
    query = update.message.text.split()[1:]
    if len(query) != 2:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Try again!"
        )
        return
    net = query[0]
    period = query[1]
    if net == "testnet":
        conn = psycopg2.connect(
            host="testnet.db.explorer.indexer.near.dev",
            database="testnet_explorer",
            user="public_readonly",
            password="nearprotocol",
        )
    elif net == "mainnet":
        conn = psycopg2.connect(
            host="mainnet.db.explorer.indexer.near.dev",
            database="mainnet_explorer",
            user="public_readonly",
            password="nearprotocol",
        )
    else:
        print("Wrong net")
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Try again!"
        )
        return

    cur = conn.cursor()
    if period == "1m":
        timestamp = int(time.time() * second_to_nanoseconds - minute_to_nanoseconds)
    elif period == "5m":
        timestamp = int(time.time() * second_to_nanoseconds - minute_to_nanoseconds * 5)
    elif period == "15m":
        timestamp = int(
            time.time() * second_to_nanoseconds - minute_to_nanoseconds * 15
        )
    elif period == "30m":
        timestamp = int(
            time.time() * second_to_nanoseconds - minute_to_nanoseconds * 30
        )
    elif period == "1h":
        timestamp = int(time.time() * second_to_nanoseconds - hour_to_nanoseconds)
    elif period == "6h":
        timestamp = int(time.time() * second_to_nanoseconds - hour_to_nanoseconds * 6)
    elif period == "12h":
        timestamp = int(time.time() * second_to_nanoseconds - hour_to_nanoseconds * 12)
    elif period == "1d":
        timestamp = int(time.time() * second_to_nanoseconds - hour_to_nanoseconds * 24)
    else:
        print("Wront timestemp")
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="Try again!"
        )
        return

    cur.execute(
        f"""
              select 
                  emitted_at_block_timestamp as time,
                  emitted_by_contract_account_id as contract,
                  token_id as token,
                  event_kind as event,
                  token_old_owner_account_id as old,
                  token_new_owner_account_id as new
              from 
                  assets__non_fungible_token_events t
              where
                  emitted_at_block_timestamp > {timestamp} and 
                  event_kind = 'MINT'
              """
    )

    mints = dict()
    unique_mints = dict()
    for mint in cur.fetchall():
        if mints.get(mint[1], -1) == -1:
            mints[mint[1]] = 1
        else:
            mints[mint[1]] += 1
        if unique_mints.get(mint[1], -1) == -1:
            unique_mints[mint[1]] = set()
        unique_mints[mint[1]].add(mint[5])

    feed = []
    for (key, value) in mints.items():
        feed.append((key, value, len(unique_mints[key])))
    feed.sort(key=cmp, reverse=True)
    feed = feed[:20]

    message = ""
    for (collection, mint, unique) in feed:
        message += f"Collection: {collection}\nMints: {mint}\nUnique mints: {unique}\n======================\n"

    if len(feed) == 0:
        message = "No NFT's were minted during this period("
    await context.bot.send_message(chat_id=update.effective_chat.id, text=message)


if __name__ == "__main__":
    application = ApplicationBuilder().token(bot_token).build()
    start_handler = CommandHandler("start", start)
    help_handler = CommandHandler("help", help)
    getfeed_handler = CommandHandler("get_feed", get_feed)
    application.add_handler(start_handler)
    application.add_handler(help_handler)
    application.add_handler(getfeed_handler)

    application.run_polling()
