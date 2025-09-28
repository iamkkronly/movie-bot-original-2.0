# © 2025 Kaustav Ray. All rights reserved.
# Licensed under the MIT License.

import logging
import asyncio
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.error import TelegramError
from fuzzywuzzy import fuzz
import math
import re
import io

# ========================
# CONFIG
# ========================
BOT_TOKEN = "8410215954:AAE0icLhQeXs4aIU0pA_wrhMbOOziPQLx24"  # Bot Token
DB_CHANNEL = -1002975831610  # Database channel
LOG_CHANNEL = -1002988891392  # Channel to log user queries
ALLOWED_GROUP_ID = -1001234567890  # The only group ID the bot will respond in
# Channels users must join for access
JOIN_CHECK_CHANNEL = [-1002692055617, -1002551875503, -1002839913869]
ADMINS = [6705618257]        # Admin IDs

# Custom promotional message (Simplified as per the last request)
CUSTOM_PROMO_MESSAGE = (
    "Credit to Prince Kaustav Ray\n\n"
    "Join our main channel: @filestore4u\n"
    "Join our channel: @code_boost\n"
    "Join our channel: @krbook_official"
)

# A list of MongoDB URIs to use. Add as many as you need.
MONGO_URIS = [
    "mongodb+srv://bf44tb5_db_user:RhyeHAHsTJeuBPNg@cluster0.lgao3zu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://28c2kqa_db_user:IL51mem7W6g37mA5@cluster0.np0ffl0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://l6yml41j_db_user:2m5HFR6CTdSb46ck@cluster0.nztdqdr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://7afcwd6_db_user:sOthaH9f53BDRBoj@cluster0.m9d2zcy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://x05bq9p_db_user:gspcMp5M0NQnu9zt@cluster0.bhxd7dp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://fxexlqy_db_user:O5HiYEZee2pyUyGK@cluster0.ugozkfc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://vnitm0p_db_user:rz1Szy1U9fwJMkis@cluster0.apaqaef.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://mezojs2_db_user:gvT09wd648MfGP5W@cluster0.c5hejzo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://wspprp42_db_user:Mac4xZJVHOxkKzK0@cluster0.cgxjhpt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://r7fyvtce_db_user:5HSZsUd5TTQSpU5V@cluster0.9l4g28a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://kvr0j2wk_db_user:wH8jseEyDSHcm35L@cluster0.mwhmepa.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://4yxduh8_db_user:45Lyw2zgcCUhxTQd@cluster0.afxbyeo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    "mongodb+srv://zdqmu6ir_db_user:gNGahCtkshRz0T6i@cluster0.ihuljbb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
]
current_uri_index = 0

mongo_client = None
db = None
files_col = None
users_col = None
banned_users_col = None


# Logging setup with an in-memory buffer for the /log command
log_stream = io.StringIO()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO, stream=log_stream
)
logger = logging.getLogger(__name__)


# ========================
# HELPERS
# ========================

def escape_markdown(text: str) -> str:
    """Helper function to escape special characters in Markdown V2."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return "".join('\\' + char if char in escape_chars else char for char in text)

def format_size(size_in_bytes: int) -> str:
    """Converts a size in bytes to a human-readable format."""
    if size_in_bytes is None:
        return "N/A"

    if size_in_bytes == 0:
        return "0 B"

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_in_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_in_bytes / p, 2)
    return f"{s} {size_name[i]}"


def format_filename_for_display(filename: str) -> str:
    """Splits a long filename into two lines for better display."""
    if len(filename) < 40:
        return filename

    mid = len(filename) // 2
    split_point = -1

    # Try to find a space near the midpoint
    for i in range(mid, 0, -1):
        if filename[i] == ' ':
            split_point = i
            break

    if split_point == -1:
        for i in range(mid, len(filename)):
            if filename[i] == ' ':
                split_point = i
                break

    if split_point != -1:
        return filename[:split_point] + '\n' + filename[split_point+1:]
    else:
        # Fallback if no space is found (e.g., a single long word)
        return filename[:mid] + '\n' + filename[mid:]

async def check_member_status(user_id, context: ContextTypes.DEFAULT_TYPE):
    """Check if the user is a member of ALL required channels."""
    for channel_id in JOIN_CHECK_CHANNEL:
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                return False
        except TelegramError as e:
            logger.error(f"Error checking member status for user {user_id} in channel {channel_id}: {e}")
            return False

    return True

async def is_banned(user_id):
    """Check if the user is banned."""
    if banned_users_col is not None:
        return banned_users_col.find_one({"_id": user_id}) is not None
    return False

async def bot_can_respond(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Check if the bot should respond in a group chat.
    - Allows all private chats.
    - In groups, responds only in the ALLOWED_GROUP_ID and only if the bot is an administrator.
    """
    chat = update.effective_chat

    if chat.type == "private":
        return True

    if chat.type in ["group", "supergroup"]:
        # First, check if the group is the allowed one.
        if chat.id != ALLOWED_GROUP_ID:
            logger.info(f"Ignoring message from non-allowed group {chat.id}.")
            return False

        # If it's the correct group, then check for admin status.
        try:
            bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
            if bot_member.status == "administrator":
                return True
            else:
                logger.info(f"Bot is not an admin in the allowed group {chat.id}, ignoring message.")
                return False
        except TelegramError as e:
            logger.error(f"Could not check bot status in group {chat.id}: {e}")
            return False

    return False

def connect_to_mongo():
    """Connect to the MongoDB URI at the current index."""
    global mongo_client, db, files_col, users_col, banned_users_col
    try:
        uri = MONGO_URIS[current_uri_index]
        # Set serverSelectionTimeoutMS to 5 seconds to fail fast if the connection is dead
        mongo_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # The ismaster command is cheap and does not require auth.
        # It forces the client to check the connection.
        mongo_client.admin.command('ismaster')

        db = mongo_client["telegram_files"]
        files_col = db["files"]
        users_col = db["users"]
        banned_users_col = db["banned_users"]
        logger.info(f"Successfully connected to MongoDB at index {current_uri_index}.")
        return True
    except (PyMongoError, IndexError) as e:
        logger.error(f"Failed to connect to MongoDB at index {current_uri_index}: {e}")
        return False

async def save_user_info(user: Update.effective_user):
    """Saves user information to the database if not already present."""
    if users_col is not None:
        try:
            users_col.update_one(
                {"_id": user.id},
                {
                    "$set": {
                        "first_name": user.first_name,
                        "last_name": user.last_name,
                        "username": user.username,
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving user info for {user.id}: {e}")


# ========================
# TASK FUNCTIONS (FOR BACKGROUND EXECUTION)
# ========================

async def send_file_task(query, context, file_data):
    """Background task to send a single file to the user's private chat and auto-delete it."""
    user_id = query.from_user.id
    try:
        sent_message = await context.bot.copy_message(
            chat_id=user_id, # Ensure it's sent to the user's private chat
            from_chat_id=file_data["channel_id"],
            message_id=file_data["file_id"],
        )

        # If the message was sent successfully
        if sent_message:
            # Send the custom combined promotional message to the private chat (now simplified)
            await context.bot.send_message(
                chat_id=user_id,
                text=CUSTOM_PROMO_MESSAGE
            )

            # Notify the user in the original chat (can be group/private)
            await query.message.reply_text("✅ I have sent the file to you in a private message. The file will be deleted automatically in 5 minutes.")

            # Wait for 5 minutes
            await asyncio.sleep(5 * 60)

            # Delete the message
            await context.bot.delete_message(
                chat_id=user_id,
                message_id=sent_message.message_id
            )
            logger.info(f"Deleted message {sent_message.message_id} from chat {user_id}.")

    except TelegramError as e:
        logger.error(f"Failed to send file to user {user_id}: {e}")
        # Only reply in the chat where the button was clicked (group/private)
        await query.message.reply_text("❌ File not found or could not be sent. Please ensure the bot is not blocked in your private chat.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while sending the file: {e}")
        await query.message.reply_text("❌ An unexpected error occurred. Please try again later.")


async def send_all_files_task(query, context, file_list):
    """Background task to send multiple files to the user's private chat and auto-delete them."""
    user_id = query.from_user.id

    # Send all files
    sent_messages = []
    try:
        for file in file_list:
            # Ensure it's sent to the user's private chat
            sent_message = await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=file["channel_id"],
                message_id=file["file_id"],
            )
            sent_messages.append(sent_message.message_id)

            # Send the custom promotional message to the user's private chat after each file (now simplified)
            await context.bot.send_message(
                chat_id=user_id,
                text=CUSTOM_PROMO_MESSAGE
            )

            # Add a small delay between sending files to avoid rate limits
            await asyncio.sleep(0.5)

        # Send final confirmation message to the chat where the button was clicked
        await query.message.reply_text(
            "✅ I have sent all files to you in a private message. The files will be deleted automatically in 5 minutes."
        )

        # Wait for 5 minutes
        await asyncio.sleep(5 * 60)

        # Delete all sent messages
        for message_id in sent_messages:
            try:
                await context.bot.delete_message(
                    chat_id=user_id,
                    message_id=message_id
                )
                logger.info(f"Deleted message {message_id} from chat {user_id}.")
            except TelegramError as e:
                logger.warning(f"Failed to delete message {message_id} for user {user_id}: {e}")

    except TelegramError as e:
        logger.error(f"Failed to send one or more files to user {user_id}: {e}")
        await query.message.reply_text("❌ One or more files could not be sent. Please ensure the bot is not blocked in your private chat.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while sending all files: {e}")
        await query.message.reply_text("❌ An unexpected error occurred. Please try again later.")

# ========================
# COMMAND HANDLERS
# ========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await update.message.reply_text("❌ You are banned from using this bot.")
        return
    await save_user_info(update.effective_user)

    user = update.effective_user
    bot_username = context.bot.username
    # Assuming the first admin in the list is the owner
    owner_id = ADMINS[0] if ADMINS else None

    welcome_text = (
        f"<b>Hey, {user.mention_html()}!</b>\n\n"
        "This is an advanced and powerful filter bot.\n\n"
        "<b><u>Your Details:</u></b>\n"
        f"<b>First Name:</b> {user.first_name}\n"
        f"<b>Last Name:</b> {user.last_name or 'N/A'}\n"
        f"<b>User ID:</b> <code>{user.id}</code>\n"
        f"<b>Username:</b> @{user.username or 'N/A'}"
    )

    keyboard = [
        [
            InlineKeyboardButton("About Bot", callback_data="start_about"),
            InlineKeyboardButton("Help", callback_data="start_help")
        ],
        [
            InlineKeyboardButton("➕ Add Me To Your Group ➕", url=f"https://t.me/{bot_username}?startgroup=true")
        ],
        [
            InlineKeyboardButton("Owner", url=f"tg://user?id={owner_id}") if owner_id else InlineKeyboardButton("Owner", callback_data="no_owner")
        ],
        [
            InlineKeyboardButton("Close", callback_data="start_close")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        welcome_text,
        reply_markup=reply_markup,
        parse_mode="HTML"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the help message and available commands."""
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await update.message.reply_text("❌ You are banned from using this bot.")
        return
    help_message = (
        "Here is a list of available commands optimized for this server:\n\n"
        "• /start - 𝑇𝑜 𝑠𝑡𝑎𝑟𝑡 𝑡ℎ𝑒 𝑏𝑜𝑡\n"
        "• /setskip - 𝑇𝑜 𝑠𝑘𝑖𝑝 𝑛𝑢𝑚𝑏𝑒𝑟 𝑜𝑓 𝑚𝑒𝑠𝑠𝑎𝑔𝑒𝑠 𝑤ℎ𝑒𝑛 𝑖𝑛𝑑𝑒𝑥𝑖𝑛𝑔 𝑓𝑖𝑙𝑒𝑠\n"
        "• /logs - 𝑡𝑜 𝑔𝑒𝑡 𝑡ℎ𝑒 𝑟𝑒𝑐𝑒𝑛𝑡 𝑒𝑟𝑟𝑜𝑟𝑠\n"
        "• /stats - 𝑡𝑜 𝑔𝑒𝑡 𝑠𝑡𝑎𝑡𝑢𝑠 𝑜𝑓 𝑓𝑖𝑙𝑒𝑠 𝑖𝑛 𝑑𝑏.\n"
        "• /connections - 𝑇𝑜 𝑠𝑒𝑒 𝑎𝑙𝑙 𝑐𝑜𝑛𝑛𝑒𝑐𝑡𝑒𝑑 𝑔𝑟𝑜𝑢𝑝𝑠\n"
        "• /settings - 𝑇𝑜 𝑜𝑝𝑒𝑛 𝑠𝑒𝑡𝑡𝑖𝑛𝑔𝑠 𝑚𝑒𝑛𝑢\n"
        "• /filter - 𝑎𝑑𝑑 𝑚𝑎𝑛𝑢𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠\n"
        "• /filters - 𝑣𝑖𝑒𝑤 𝑓𝑖𝑙𝑡𝑒𝑟𝑠\n"
        "• /connect - 𝑐𝑜𝑛𝑛𝑒𝑐𝑡 𝑡𝑜 𝑃𝑀.\n"
        "• /disconnect - 𝑑𝑖𝑠𝑐𝑜𝑛𝑛𝑒𝑐𝑡 𝑓𝑟𝑜𝑚 𝑃𝑀\n"
        "• /del - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎 𝑓𝑖𝑙𝑡𝑒𝑟\n"
        "• /delall - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎𝑙𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠\n"
        "• /deleteall - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎𝑙𝑙 𝑖𝑛𝑑𝑒𝑥𝑒𝑑 𝑓𝑖𝑙𝑒𝑠.\n"
        "• /delete - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎 𝑠𝑝𝑒𝑐𝑖𝑓𝑖𝑐 𝑓𝑖𝑙𝑒 𝑓𝑟𝑜𝑚 𝑖𝑛𝑑𝑒𝑥.\n"
        "• /info - 𝑔𝑒𝑡 𝑢𝑠𝑒𝑟 𝑖𝑛𝑓𝑜\n"
        "• /id - 𝑔𝑒𝑡 𝑡𝑔 𝑖𝑑𝑠.\n"
        "• /imdb - 𝑓𝑒𝑡𝑐ℎ 𝑖𝑛𝑓𝑜 𝑓𝑟𝑜𝑚 𝑖𝑚𝑑𝑏.\n"
        "• /users - 𝑡𝑜 𝑔𝑒𝑡 𝑙𝑖𝑠𝑡 𝑜𝑓 𝑚𝑦 𝑢𝑠𝑒𝑟𝑠 𝑎𝑛𝑑 𝑖𝑑𝑠.\n"
        "• /chats - 𝑡𝑜 𝑔𝑒𝑡 𝑙𝑖𝑠𝑡 𝑜𝑓 𝑡ℎ𝑒 𝑚𝑦 𝑐ℎ𝑎𝑡𝑠 𝑎𝑛𝑑 𝑖𝑑𝑠 \n"
        "• /leave  - 𝑡𝑜 𝑙𝑒𝑎𝑣𝑒 𝑓𝑟𝑜𝑚 𝑎 𝑐ℎ𝑎𝑡.\n"
        "• /disable  -  𝑑𝑜 𝑑𝑖𝑠𝑎𝑏𝑙𝑒 𝑎 𝑐ℎ𝑎𝑡.\n"
        "• /enable - 𝑟𝑒-𝑒𝑛𝑎𝑏𝑙𝑒 𝑐ℎ𝑎𝑡.\n"
        "• /ban  - 𝑡𝑜 𝑏𝑎𝑛 𝑎 𝑢𝑠𝑒𝑟.\n"
        "• /unban  - 𝑡𝑜 𝑢𝑛𝑏𝑎𝑛 𝑎 𝑢𝑠𝑒𝑟.\n"
        "• /channel - 𝑡𝑜 𝑔𝑒𝑡 𝑙𝑖𝑠𝑡 𝑜𝑓 𝑡𝑜𝑡𝑎𝑙 𝑐𝑜𝑛𝑛𝑒𝑐𝑡𝑒𝑑 𝑐ℎ𝑎𝑛𝑛𝑒𝑙𝑠\n"
        "• /broadcast - 𝑡𝑜 𝑏𝑟𝑜𝑎𝑑𝑐𝑎𝑠𝑡 𝑎 𝑚𝑒𝑠𝑠𝑎𝑔𝑒 𝑡𝑜 𝑎𝑙𝑙 𝑢𝑠𝑒𝑟𝑠\n"
        "• /grp_broadcast - 𝑇𝑜 𝑏𝑟𝑜𝑎𝑑𝑐𝑎𝑠𝑡 𝑎 𝑚𝑒𝑠𝑠𝑎𝑔𝑒 𝑡𝑜 𝑎𝑙𝑙 𝑐𝑜𝑛𝑛𝑒𝑐𝑡𝑒𝑑 𝑔𝑟𝑜𝑢𝑝𝑠.\n"
        "• /batch - 𝑡𝑜 𝑐𝑟𝑒𝑎𝑡𝑒 𝑙𝑖𝑛𝑘 𝑓𝑜𝑟 𝑚𝑢𝑙𝑡𝑖𝑝𝑙𝑒 𝑝𝑜𝑠𝑡𝑠\n"
        "• /link - 𝑡𝑜 𝑐𝑟𝑒𝑎𝑡𝑒 𝑙𝑖𝑛𝑘 𝑓𝑜𝑟 𝑜𝑛𝑒 𝑝𝑜𝑠𝑡\n"
        "• /set_template - 𝑇𝑜 𝑠𝑒𝑡 𝑎 𝑐𝑢𝑠𝑡𝑜𝑚 𝐼𝑀𝐷𝑏 𝑡𝑒𝑚𝑝𝑙𝑎𝑡𝑒 𝑓𝑜𝑟 𝑖𝑛𝑑𝑖𝑣𝑖𝑑𝑢𝑎𝑙 𝑔𝑟𝑜𝑢𝑝𝑠\n"
        "• /gfilter - 𝑇𝑜 𝑎𝑑𝑑 𝑔𝑙𝑜𝑏𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠.\n"
        "• /gfilters - 𝑇𝑜 𝑣𝑖𝑒𝑤 𝑙𝑖𝑠𝑡 𝑜𝑓 𝑎𝑙𝑙 𝑔𝑙𝑜𝑏𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠.\n"
        "• /delg - 𝑇𝑜 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎 𝑠𝑝𝑒𝑐𝑖𝑓𝑖𝑐 𝑔𝑙𝑜𝑏𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟.\n"
        "• /delallg - 𝑇𝑜 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎𝑙𝑙 𝑔𝑙𝑜𝑏𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠 𝑓𝑟𝑜𝑚 𝑡ℎ𝑒 𝑏𝑜𝑡'𝑠 𝑑𝑎𝑡𝑎𝑏𝑎𝑠𝑒.\n"
        "• /deletefiles - 𝑇𝑜 𝑑𝑒𝑙𝑒𝑡𝑒 𝑃𝑟𝑒𝐷𝑉𝐷 𝑎𝑛𝑑 𝐶𝑎𝑚𝑅𝑖𝑝 𝐹𝑖𝑙𝑒𝑠 𝑓𝑟𝑜𝑚 𝑡ℎ𝑒 𝑏𝑜𝑡'𝑠 𝑑𝑎𝑡𝑎𝑏𝑎𝑠𝑒.\n"
        "• /add_premium - 𝐴𝑑𝑑 𝑢𝑠𝑒𝑟 𝑡𝑜 𝑝𝑟𝑒𝑚𝑖𝑢𝑚 𝑙𝑖𝑠𝑡\n"
        "• /remove_premium - 𝑅𝑒𝑚𝑜𝑣𝑒 𝑢𝑠𝑒𝑟 𝑓𝑟𝑜𝑚 𝑝𝑟𝑒𝑚𝑖𝑢𝑚 𝑙𝑖𝑠𝑡\n"
        "• /plan - 𝐶ℎ𝑒𝑐𝑘 𝑝𝑙𝑎𝑛 𝑑𝑒𝑡𝑎𝑖𝑙𝑠\n"
        "• /myplan - 𝐶ℎ𝑒𝑐𝑘 𝑦𝑜𝑢𝑟 𝑝𝑙𝑎𝑛 𝑠𝑡𝑎𝑡𝑠\n"
        "• /shortlink - 𝑠𝑒𝑡 𝑦𝑜𝑢𝑟 𝑢𝑟𝑙 𝑠ℎ𝑜𝑟𝑡𝑛𝑒𝑟 𝑖𝑛 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝\n"
        "• /setshortlinkoff  - 𝑜𝑓𝑓 𝑠ℎ𝑜𝑟𝑡𝑙𝑖𝑛𝑘 𝑖𝑛 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝\n"
        "• /setshortlinkon - 𝑜𝑛 𝑠ℎ𝑜𝑟𝑡𝑙𝑖𝑛𝑘 𝑖𝑛 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝\n"
        "• /shortlink_info - 𝑐ℎ𝑒𝑐𝑘 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝 𝑎𝑙𝑙 𝑠ℎ𝑜𝑟𝑡𝑙𝑖𝑛𝑘 𝑎𝑛𝑑 𝑡𝑢𝑡𝑜𝑟𝑖𝑎𝑙 𝑙𝑖𝑛𝑘 𝑑𝑒𝑡𝑎𝑖𝑙𝑠\n"
        "• /set_tutorial - 𝑠𝑒𝑡 𝑦𝑜𝑢𝑟 𝑢𝑟𝑙 𝑠ℎ𝑜𝑟𝑡𝑛𝑒𝑟 ℎ𝑜𝑤 𝑡𝑜 𝑜𝑝𝑒𝑛 𝑙𝑖𝑛𝑘 𝑢𝑟𝑙\n"
        "• /remove_tutorial - 𝑟𝑒𝑚𝑜𝑣𝑒 𝑦𝑜𝑢𝑟 𝑡𝑢𝑡𝑜𝑟𝑖𝑎𝑙 𝑢𝑟𝑙\n"
        "• /fsub - 𝑎𝑑𝑑 𝑓𝑜𝑟𝑐𝑒 𝑠𝑢𝑏𝑠𝑐𝑟𝑖𝑏𝑒 𝑐ℎ𝑎𝑛𝑛𝑒𝑙 𝑖𝑛 𝑔𝑟𝑜𝑢𝑝\n"
        "• /nofsub - 𝑟𝑒𝑚𝑜𝑣𝑒 𝑜𝑟 𝑜𝑓𝑓 𝑓𝑜𝑟𝑐𝑒 𝑠𝑢𝑏𝑠𝑐𝑟𝑖𝑏𝑒 𝑖𝑛 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝\n"
        "• /rename - 𝑟𝑒𝑛𝑎𝑚𝑒 𝑦𝑜𝑢𝑟 𝑓𝑖𝑙𝑒\n"
        "• /set_caption - 𝑎𝑑𝑑 𝑐𝑎𝑝𝑡𝑖𝑜𝑛 𝑓𝑜𝑟 𝑦𝑜𝑢𝑟 𝑟𝑒𝑛𝑎𝑚𝑒𝑑 𝑓𝑖𝑙𝑒\n"
        "• /see_caption - 𝑠𝑒𝑒 𝑦𝑜𝑢𝑟 𝑠𝑎𝑣𝑒𝑑 𝑐𝑎𝑝𝑡𝑖𝑜𝑛\n"
        "• /del_caption - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑦𝑜𝑢𝑟 𝑠𝑎𝑣𝑒𝑑 𝑐𝑎𝑝𝑡𝑖𝑜𝑛\n"
        "• /set_thumb - 𝑎𝑑𝑑 𝑡ℎ𝑢𝑚𝑏𝑛𝑎𝑖𝑙 𝑓𝑜𝑟 𝑦𝑜𝑢𝑟 𝑟𝑒𝑛𝑎𝑚𝑒𝑑 𝑓𝑖𝑙𝑒\n"
        "• /view_thumb - 𝑣𝑖𝑒𝑤 𝑦𝑜𝑢𝑟 𝑠𝑎𝑣𝑒𝑑 𝑡ℎ𝑢𝑚𝑏𝑛𝑎𝑖𝑙\n"
        "• /del_thumb - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑦𝑜𝑢𝑟 𝑠𝑎𝑣𝑒𝑑 𝑡ℎ𝑢𝑚𝑏𝑛𝑎𝑖𝑙\n"
        "• /stream - 𝑔𝑒𝑛𝑒𝑟𝑎𝑡𝑒 𝑠𝑡𝑟𝑒𝑎𝑚 𝑎𝑛𝑑 𝑑𝑜𝑤𝑛𝑙𝑜𝑎𝑑 𝑙𝑖nk 𝑜𝑓 𝑦𝑜𝑢𝑟 𝑓𝑖𝑙𝑒\n"
        "• /telegraph - 𝑔𝑒𝑡 𝑡𝑒𝑙𝑒𝑔𝑟𝑎𝑝ℎ 𝑙𝑖𝑛𝑘 𝑜𝑓 𝑎𝑛𝑦 𝑓𝑖𝑙𝑒 𝑢𝑛𝑑𝑒𝑟 5𝑚𝑏\n"
        "• /stickerid - 𝑡𝑜 𝑔𝑒𝑡 𝑖𝑑 𝑎𝑛𝑑 𝑢𝑛𝑖𝑞𝑢𝑒 𝐼'𝑑 𝑜𝑓 𝑠𝑡𝑖𝑐𝑘𝑒𝑟\n"
        "• /font - 𝑡𝑜 𝑔𝑒𝑡 𝑎𝑛𝑦 𝑡𝑦𝑝𝑒 𝑜𝑓 𝑓𝑜𝑛𝑡 𝑜𝑓 𝑎𝑛𝑦 𝑤𝑜𝑟𝑑\n"
        "• /repo - 𝑔𝑒𝑡 𝑎𝑛𝑦 𝑟𝑒𝑝𝑜 𝑙𝑖𝑛𝑘 𝑏𝑦 𝑠𝑒𝑎𝑟𝑐ℎ𝑖𝑛𝑔\n"
        "• /purgerequests - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎𝑙𝑙 𝑗𝑜𝑖𝑛 𝑟𝑒𝑞𝑢𝑒𝑠𝑡𝑠 𝑓𝑟𝑜𝑚 𝑑𝑎𝑡𝑎𝑏𝑎𝑠𝑒\n"
        "• /totalrequests - 𝑔𝑒𝑡 𝑡𝑜𝑡𝑎𝑙 𝑛𝑢𝑚𝑏𝑒𝑟 𝑜𝑓 𝑗𝑜𝑖𝑛 𝑟𝑒𝑞𝑢𝑒𝑠𝑡 𝑓𝑟𝑜𝑚 𝑑𝑎𝑡𝑎𝑏𝑎𝑠𝑒\n"
    )
    await update.message.reply_text(help_message)


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows information about the bot."""
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await update.message.reply_text("❌ You are banned from using this bot.")
        return
    info_message = (
        "**About this Bot**\n\n"
        "This bot helps you find and share files on Telegram.\n"
        "• Developed by Kaustav Ray."
    )
    await update.message.reply_text(info_message, parse_mode="Markdown")


async def log_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show recent error logs."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    # Retrieve all logs from the in-memory stream
    log_stream.seek(0)
    logs = log_stream.readlines()

    # Filter for ERROR and CRITICAL logs and get the last 20
    error_logs = [log.strip() for log in logs if "ERROR" in log or "CRITICAL" in log]
    recent_errors = error_logs[-20:]

    if not recent_errors:
        await update.message.reply_text("✅ No recent errors found in the logs.")
    else:
        log_text = "```\nRecent Error Logs:\n\n" + "\n".join(recent_errors) + "\n```"
        await update.message.reply_text(log_text, parse_mode="MarkdownV2")

    # Clear the log buffer to prevent it from growing too large
    log_stream.seek(0)
    log_stream.truncate(0)


async def total_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get the total number of users."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    if users_col is None:
        await update.message.reply_text("❌ Database not connected.")
        return

    try:
        user_count = users_col.count_documents({})
        await update.message.reply_text(f"📊 **Total Users:** {user_count}")
    except Exception as e:
        logger.error(f"Error getting user count: {e}")
        await update.message.reply_text("❌ Failed to retrieve user count. Please check the database connection.")


async def total_files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get the total number of files."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    if files_col is None:
        await update.message.reply_text("❌ Database not connected.")
        return

    try:
        # NOTE: This only gives the count from the CURRENT active database.
        file_count = files_col.count_documents({})
        await update.message.reply_text(f"🗃️ **Total Files (Current DB):** {file_count}")
    except Exception as e:
        logger.error(f"Error getting file count: {e}")
        await update.message.reply_text("❌ Failed to retrieve file count. Please check the database connection.")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get bot statistics, including per-URI file counts. (MODIFIED)"""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    await update.message.reply_text("🔄 Collecting statistics, please wait...")

    user_count = 0
    total_file_count_all_db = 0 # Accumulator for total files across all URIs
    uri_stats = {}

    try:
        # 1. Get Total Users (from the currently connected DB)
        if users_col is not None:
            user_count = users_col.count_documents({})

        # 2. Get File Counts per URI and total file count
        for idx, uri in enumerate(MONGO_URIS):
            temp_client = None
            try:
                # Temporarily connect to each URI
                temp_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                temp_client.admin.command('ismaster')
                temp_db = temp_client["telegram_files"]
                temp_files_col = temp_db["files"]
                # Use estimated_document_count for fast approximation
                file_count = temp_files_col.estimated_document_count()
                uri_stats[idx] = f"✅ {file_count} files"
                total_file_count_all_db += file_count # Accumulate count
            except Exception as e:
                logger.warning(f"Failed to connect or get file count for URI #{idx + 1}: {e}")
                uri_stats[idx] = "❌ Failed to connect/read"
            finally:
                if temp_client:
                    temp_client.close()

        # 3. Format the output message
        stats_message = (
            f"📊 **Bot Statistics**\n"
            f"  • Total Users: {user_count}\n"
            f"  • Total Connected Groups: {len(JOIN_CHECK_CHANNEL)}\n" # Using the count of JOIN_CHECK_CHANNEL
            f"  • Total Files (All DB): {total_file_count_all_db}\n" # Total count from all URIs
            f"  • **Total MongoDB URIs:** {len(MONGO_URIS)}\n"
            f"  • **Current Active URI:** #{current_uri_index + 1}\n\n"
            f"**File Count per URI:**\n"
        )
        for idx, status in uri_stats.items():
            stats_message += f"  • URI #{idx + 1}: {status}\n"

        await update.message.reply_text(stats_message, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error getting bot stats: {e}")
        await update.message.reply_text("❌ Failed to retrieve statistics. Please check the database connection.")


async def delete_file_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to delete a file by its MongoDB ID."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    if files_col is None:
        await update.message.reply_text("❌ Database not connected.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /deletefile <MongoDB_ID>\nTip: Use /findfile <filename> to get the ID.")
        return

    try:
        file_id = context.args[0]
        # NOTE: This only deletes from the *current* active database.
        result = files_col.delete_one({"_id": ObjectId(file_id)})

        if result.deleted_count == 1:
            await update.message.reply_text(f"✅ File with ID `{file_id}` has been deleted from the database.")
        else:
            await update.message.reply_text(f"❌ File with ID `{file_id}` not found in the database.")
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        await update.message.reply_text("❌ Invalid ID or an error occurred. Please provide a valid MongoDB ID.")


async def find_file_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to find a file by its name and show its ID. Searches ALL URIs."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /findfile <filename>")
        return

    query_filename = " ".join(context.args)
    all_results = []

    await update.message.reply_text(f"🔎 Searching all {len(MONGO_URIS)} databases for `{query_filename}`...")

    # Iterate through all URIs
    for idx, uri in enumerate(MONGO_URIS):
        temp_client = None
        try:
            temp_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            temp_client.admin.command('ismaster')
            temp_db = temp_client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Use regex for case-insensitive search
            results = list(temp_files_col.find({"file_name": {"$regex": query_filename, "$options": "i"}}))
            all_results.extend(results)
            logger.info(f"Found {len(results)} files in URI #{idx + 1}")
        except Exception as e:
            logger.error(f"Error finding file on URI #{idx + 1}: {e}")
        finally:
            if temp_client:
                temp_client.close()


    if not all_results:
        await update.message.reply_text(f"❌ No files found with the name `{query_filename}` in any database.")
        return

    response_text = f"📁 Found {len(all_results)} files matching `{query_filename}` across all databases:\n\n"
    for idx, file in enumerate(all_results):
        response_text += f"{idx + 1}. *{escape_markdown(file['file_name'])}*\n  `ID: {file['_id']}`\n\n"

    response_text += "Copy the ID of the file you want to delete and use the command:\n`/deletefile <ID>`\n\nNote: `/deletefile` only works on the currently *active* database. If the file is not found, you may need to manually update the `current_uri_index` and restart."

    await update.message.reply_text(response_text, parse_mode="Markdown")


async def delete_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to delete all files from the database."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    if files_col is None:
        await update.message.reply_text("❌ Database not connected.")
        return

    try:
        # NOTE: This only deletes from the *current* active database.
        result = files_col.delete_many({})
        await update.message.reply_text(f"✅ Deleted {result.deleted_count} files from the **current** database.")
    except Exception as e:
        logger.error(f"Error deleting all files: {e}")
        await update.message.reply_text("❌ An error occurred while trying to delete all files from the current database.")


async def ban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to ban a user by their user ID."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /ban <user_id>")
        return

    user_to_ban_id = int(context.args[0])
    if user_to_ban_id in ADMINS:
        await update.message.reply_text("❌ Cannot ban an admin.")
        return

    if banned_users_col is None:
        await update.message.reply_text("❌ Database not connected.")
        return

    try:
        banned_users_col.update_one(
            {"_id": user_to_ban_id},
            {"$set": {"_id": user_to_ban_id}},
            upsert=True
        )
        await update.message.reply_text(f"🔨 User `{user_to_ban_id}` has been banned.")
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await update.message.reply_text("❌ An error occurred while trying to ban the user.")


async def unban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to unban a user by their user ID."""
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /unban <user_id>")
        return

    user_to_unban_id = int(context.args[0])

    if banned_users_col is None:
        await update.message.reply_text("❌ Database not connected.")
        return

    try:
        result = banned_users_col.delete_one({"_id": user_to_unban_id})

        if result.deleted_count == 1:
            await update.message.reply_text(f"✅ User `{user_to_unban_id}` has been unbanned.")
        else:
            await update.message.reply_text(f"❌ User `{user_to_unban_id}` was not found in the banned list.")
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await update.message.reply_text("❌ An error occurred while trying to unban the user.")


async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Broadcasts a message to all users in the database.
    Usage: /broadcast <message>
    """
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("❌ You do not have permission to use this command.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return

    broadcast_text = " ".join(context.args)

    # NOTE: This only broadcasts to users in the *current* active database's users_col.
    # To broadcast to ALL users, you'd need to query all URIs for user IDs.
    users_cursor = users_col.find({}, {"_id": 1})
    user_ids = [user["_id"] for user in users_cursor]
    sent_count = 0
    failed_count = 0

    await update.message.reply_text(f"🚀 Starting broadcast to {len(user_ids)} users...")

    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=broadcast_text)
            sent_count += 1
            await asyncio.sleep(0.1)
        except TelegramError as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast to user {uid}: {e}")
        except Exception as e:
            failed_count += 1
            logger.error(f"Unknown error sending broadcast to user {uid}: {e}")

    await update.message.reply_text(f"✅ Broadcast complete!\n\nSent to: {sent_count}\nFailed: {failed_count}")


# ========================
# FILE/SEARCH HANDLERS
# ========================

async def save_file_from_pm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sends file to bot -> save to channel + DB"""
    user_id = update.message.from_user.id
    if user_id not in ADMINS:
        return

    file = update.message.document or update.message.video or update.message.audio
    if not file:
        return

    # Forward to database channel
    forwarded = await update.message.forward(DB_CHANNEL)

    # Get filename from caption, then from file_name, replacing underscores, dots, and hyphens with spaces
    # Otherwise, use a default value
    if update.message.caption:
        raw_name = update.message.caption
    else:
        raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id

    clean_name = raw_name.replace("_", " ").replace(".", " ").replace("-", " ") if raw_name else "Unknown"

    global current_uri_index, files_col

    saved = False
    temp_uri_index = current_uri_index
    # Start the loop from the current active index and wrap around to try all
    for i in range(len(MONGO_URIS)):
        idx = (temp_uri_index + i) % len(MONGO_URIS)
        uri_to_try = MONGO_URIS[idx]

        temp_client = None
        try:
            # If we're not on the currently connected URI, we need a new client
            if idx != current_uri_index or files_col is None:
                temp_client = MongoClient(uri_to_try, serverSelectionTimeoutMS=5000)
                temp_client.admin.command('ismaster')
                temp_db = temp_client["telegram_files"]
                temp_files_col = temp_db["files"]
            else:
                # Use the global client
                temp_files_col = files_col

            # Try to save metadata
            temp_files_col.insert_one({
                "file_name": clean_name,
                "file_id": forwarded.message_id,
                "channel_id": forwarded.chat.id,
                "file_size": file.file_size,
            })

            # If successful, set the current global index to this one
            if idx != current_uri_index:
                global mongo_client, db, users_col, banned_users_col
                # Close old client if needed
                if mongo_client:
                    mongo_client.close()

                # Update global connection pointers
                mongo_client = temp_client
                db = temp_db
                files_col = temp_files_col
                current_uri_index = idx
                logger.info(f"Switched active MongoDB connection to index {current_uri_index}.")

            await update.message.reply_text(f"✅ Saved to DB #{idx + 1}: {clean_name}")
            saved = True
            break
        except Exception as e:
            logger.error(f"Error saving file with URI #{idx + 1}: {e}")
            if idx == current_uri_index and len(MONGO_URIS) > 1:
                 await update.message.reply_text(f"⚠️ Primary DB failed. Trying next available URI...")
        finally:
            if temp_client and idx != current_uri_index:
                temp_client.close()


    if not saved:
        logger.error("All MongoDB URIs have been tried and failed.")
        await update.message.reply_text("❌ Failed to save file on all available databases.")


async def save_file_from_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sends file directly to channel -> save to DB"""
    user_id = update.message.from_user.id
    chat_id = update.message.chat.id

    # Only process files from admins in the database channel
    if chat_id != DB_CHANNEL or user_id not in ADMINS:
        return

    file = update.message.document or update.message.video or update.message.audio
    if not file:
        return

    # Get filename from caption, then from file_name, replacing underscores, dots, and hyphens with spaces
    # Otherwise, use a default value
    if update.message.caption:
        raw_name = update.message.caption
    else:
        raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id

    clean_name = raw_name.replace("_", " ").replace(".", " ").replace("-", " ") if raw_name else "Unknown"

    global current_uri_index, files_col

    saved = False
    temp_uri_index = current_uri_index

    for i in range(len(MONGO_URIS)):
        idx = (temp_uri_index + i) % len(MONGO_URIS)
        uri_to_try = MONGO_URIS[idx]

        temp_client = None
        try:
            if idx != current_uri_index or files_col is None:
                temp_client = MongoClient(uri_to_try, serverSelectionTimeoutMS=5000)
                temp_client.admin.command('ismaster')
                temp_db = temp_client["telegram_files"]
                temp_files_col = temp_db["files"]
            else:
                temp_files_col = files_col

            # Try to save metadata
            temp_files_col.insert_one({
                "file_name": clean_name,
                "file_id": update.message.message_id,
                "channel_id": chat_id,
                "file_size": file.file_size,
            })

            # If successful, set the current global index to this one
            if idx != current_uri_index:
                global mongo_client, db, users_col, banned_users_col
                if mongo_client:
                    mongo_client.close()

                mongo_client = temp_client
                db = temp_db
                files_col = temp_files_col
                current_uri_index = idx
                logger.info(f"Switched active MongoDB connection to index {current_uri_index}.")

            # Send **INSTANT** success notification to the admin
            try:
                await context.bot.send_message(
                    user_id,
                    f"✅ File **`{escape_markdown(clean_name)}`** has been indexed successfully from the database channel to DB #{idx + 1}.",
                    parse_mode="MarkdownV2"
                )
            except TelegramError as e:
                logger.error(f"Failed to send notification to admin {user_id}: {e}")
            saved = True
            break

        except Exception as e:
            logger.error(f"Error saving file from channel with URI #{idx + 1}: {e}")
            if idx == current_uri_index and len(MONGO_URIS) > 1:
                try:
                    await context.bot.send_message(user_id, "⚠️ Primary DB failed. Trying next available URI...")
                except TelegramError:
                    pass
        finally:
            if temp_client and idx != current_uri_index:
                temp_client.close()

    if not saved:
        logger.error("All MongoDB URIs have been tried and failed.")
        try:
            await context.bot.send_message(user_id, "❌ Failed to save file on all available databases.")
        except TelegramError:
            pass


async def search_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Search ALL URIs and show results, sorted by relevance.
    Uses a broad regex for initial filtering and fuzzy matching for accurate ranking.
    """
    if not await bot_can_respond(update, context):
        return

    # In private chat, only admins can search for files.
    if update.effective_chat.type == "private" and update.effective_user.id not in ADMINS:
        await update.message.reply_text("❌ Sorry, only admins can use the search function in a private chat.")
        return

    if await is_banned(update.effective_user.id):
        await update.message.reply_text("❌ You are banned from using this bot.")
        return

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        # NEW: Updated to show buttons for all channels
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Join Channel: @filestore4u", url="https://t.me/filestore4u")],
            [InlineKeyboardButton("Join Channel: @code_boost", url="https://t.me/code_boost")],
            [InlineKeyboardButton("Join Channel: @krbook_official", url="https://t.me/krbook_official")]
        ])
        await update.message.reply_text("❌ You must join ALL our channels to use this bot!", reply_markup=keyboard)
        return

    # Send instant feedback
    await update.message.reply_text(f"🔍 Searching all {len(MONGO_URIS)} databases...")

    raw_query = update.message.text.strip()
    # Normalize query for better fuzzy search
    normalized_query = raw_query.replace("_", " ").replace(".", " ").replace("-", " ").strip()

    # Log the user's query
    user = update.effective_user
    log_text = f"🔍 User: {user.full_name} | @{user.username} | ID: {user.id}\nQuery: {raw_query}"
    try:
        await context.bot.send_message(LOG_CHANNEL, text=log_text)
    except Exception as e:
        logger.error(f"Failed to log query to channel: {e}")


    # --- REVISED SEARCH LOGIC (Broad Filtering + Fuzzy Ranking) ---

    # Split the query into words and escape them for a forgiving regex. Ignore short words.
    words = [re.escape(word) for word in normalized_query.split() if len(word) > 1]

    if not words:
        await update.message.reply_text("❌ Query too short or invalid. Please try a longer search term.")
        return

    # Create an OR condition for the words (e.g., /word1|word2|.../i)
    # This ensures that if ANY of the main words are in the filename, it's considered for fuzzy ranking.
    regex_pattern = re.compile("|".join(words), re.IGNORECASE)
    query_filter = {"file_name": {"$regex": regex_pattern}}

    preliminary_results = []

    # Iterate over ALL URIs for search
    for idx, uri in enumerate(MONGO_URIS):
        temp_client = None
        try:
            # Temporarily connect to each URI
            temp_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            temp_client.admin.command('ismaster')
            temp_db = temp_client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Query the database with the broad filter
            results = list(temp_files_col.find(query_filter))
            preliminary_results.extend(results)

        except Exception as e:
            logger.error(f"MongoDB search query failed on URI #{idx + 1}: {e}")
        finally:
            if temp_client:
                temp_client.close()

    # --- Fuzzy Ranking (to ensure the best match is first) ---

    if not preliminary_results:
        await update.message.reply_text("❌ No relevant files found. For your query contact @kaustavhibot")
        return

    results_with_score = []
    # Use a set to track file_id + channel_id tuples to ensure no duplicates from different DBs
    unique_files = set()

    for file in preliminary_results:
        file_key = (file.get('file_id'), file.get('channel_id'))
        if file_key in unique_files:
            continue

        # Use token_set_ratio: best for comparing strings where words might be reordered or contain extra words.
        score = fuzz.token_set_ratio(normalized_query, file['file_name'])

        # Keep results that have a score above 40 (high relevance)
        if score > 40:
            results_with_score.append((file, score))
            unique_files.add(file_key)

    # Sort the results by score in descending order
    sorted_results = sorted(results_with_score, key=lambda x: x[1], reverse=True)

    # Extract the file documents from the sorted list and limit to the top 50
    final_results = [result[0] for result in sorted_results[:50]]

    if not final_results:
        await update.message.reply_text("❌ No relevant files found after filtering by relevance. For your query contact @kaustavhibot")
        return

    # Pass the full result list to the pagination function for consistency
    context.user_data['search_results'] = final_results
    context.user_data['search_query'] = raw_query

    await send_results_page(update.effective_chat.id, final_results, 0, context, raw_query, new_message=True)


async def send_results_page(chat_id, results, page, context: ContextTypes.DEFAULT_TYPE, query: str, message_id=None, new_message=False):
    start, end = page * 10, (page + 1) * 10
    page_results = results[start:end]

    # Escape the query string for Markdown
    escaped_query = escape_markdown(query)
    text = f"🔎 *Top {len(results)}* Results for: *{escaped_query}*\n(Page {page + 1} / {math.ceil(len(results) / 10)}) (Sorted by Relevance)"
    buttons = []

    # Add files for the current page
    for idx, file in enumerate(page_results, start=start + 1):
        # Format the filename first, then escape it for Markdown
        file_size = format_size(file.get("file_size"))
        file_obj_id = str(file['_id'])

        button_text = f"[{file_size}] {file['file_name'][:40]}"
        buttons.append(
            [InlineKeyboardButton(button_text, callback_data=f"get_{file_obj_id}")]
        )

    # Add the promotional text at the end
    text += "\n\nKaustav Ray                                                                                                      Join here: @filestore4u     @freemovie5u"

    # Add navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page_{page-1}_{query}"))
    if end < len(results):
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{page+1}_{query}"))

    if nav_buttons:
        buttons.append(nav_buttons)

    # Send All button
    buttons.append([InlineKeyboardButton("📨 Send All Files (Current Page)", callback_data=f"sendall_{page}_{query}")])

    reply_markup = InlineKeyboardMarkup(buttons)

    try:
        if new_message or message_id is None:
            # Send as a new message (used for initial search result)
            await context.bot.send_message(
                chat_id, text, reply_markup=reply_markup, parse_mode="Markdown"
            )
        else:
            # Edit the existing message (used for pagination)
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
    except TelegramError as e:
        logger.error(f"Error sending/editing search results page: {e}")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button clicks"""
    # Provide an immediate answer to the callback query to clear the loading state on the button
    query = update.callback_query
    await query.answer()

    if not await bot_can_respond(update, context):
        return

    if await is_banned(update.effective_user.id):
        await query.message.reply_text("❌ You are banned from using this bot.")
        return

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Join Channel: @filestore4u", url="https://t.me/filestore4u")],
            [InlineKeyboardButton("Join Channel: @code_boost", url="https://t.me/code_boost")],
            [InlineKeyboardButton("Join Channel: @krbook_official", url="https://tme/krbook_official")]
        ])
        await query.message.reply_text("❌ You must join ALL our channels to use this bot!", reply_markup=keyboard)
        return

    data = query.data

    if data.startswith("get_"):
        # Send a message to the user to confirm the request is being processed
        await query.message.reply_text("⌛ Processing your request, please wait...")

        file_id_str = data.split("_", 1)[1]
        file_data = None

        # --- NEW LOGIC: Check ALL URIs to find the file by its MongoDB ID ---
        for uri in MONGO_URIS:
            temp_client = None
            try:
                temp_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                temp_client.admin.command('ismaster')
                temp_db = temp_client["telegram_files"]
                temp_files_col = temp_db["files"]

                # We search by ObjectId which should be unique across all database copies
                file_data = temp_files_col.find_one({"_id": ObjectId(file_id_str)})
                if file_data:
                    logger.info(f"File {file_id_str} found for retrieval in URI: {uri}")
                    break # Found the file, stop searching
            except Exception as e:
                logger.error(f"Error checking file ID {file_id_str} in URI {uri}: {e}")
            finally:
                if temp_client:
                    temp_client.close()
        # --- END NEW LOGIC ---

        if file_data:
            # Schedule the slow `send_file_task` to run in the background.
            asyncio.create_task(send_file_task(query, context, file_data))
        else:
            await query.message.reply_text("❌ File not found. The file may have been deleted or the database is inaccessible.")

    elif data.startswith("page_"):
        # This handles both 'Prev' and 'Next' clicks
        _, page_str, search_query = data.split("_", 2)
        page = int(page_str)

        # Retrieve search results from user_data
        final_results = context.user_data.get('search_results')

        if not final_results:
            # Re-run the search if results are lost (e.g., bot restarted or context cleared)
            await query.message.reply_text("⚠️ Search results lost. Re-running search...")

            # This logic is copied from the search_files function for recovery purposes
            normalized_query = search_query.replace("_", " ").replace(".", " ").replace("-", " ").strip()
            words = [re.escape(word) for word in normalized_query.split() if len(word) > 1]
            if not words:
                await query.message.reply_text("❌ Query too short or invalid.")
                return

            regex_pattern = re.compile("|".join(words), re.IGNORECASE)
            query_filter = {"file_name": {"$regex": regex_pattern}}
            preliminary_results = []

            for uri in MONGO_URIS:
                temp_client = None
                try:
                    temp_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                    temp_client.admin.command('ismaster')
                    temp_db = temp_client["telegram_files"]
                    temp_files_col = temp_db["files"]
                    results = list(temp_files_col.find(query_filter))
                    preliminary_results.extend(results)
                except Exception as e:
                    logger.error(f"MongoDB search query failed during recovery: {e}")
                finally:
                    if temp_client:
                        temp_client.close()

            results_with_score = []
            unique_files = set()
            for file in preliminary_results:
                file_key = (file.get('file_id'), file.get('channel_id'))
                if file_key in unique_files: continue
                score = fuzz.token_set_ratio(normalized_query, file['file_name'])
                if score > 40:
                    results_with_score.append((file, score))
                    unique_files.add(file_key)

            sorted_results = sorted(results_with_score, key=lambda x: x[1], reverse=True)
            final_results = [result[0] for result in sorted_results[:50]]

            if not final_results:
                await query.message.reply_text("❌ No relevant files found after recovery.")
                return

            # Save recovered results
            context.user_data['search_results'] = final_results
            context.user_data['search_query'] = search_query

        # --- THE KEY CHANGE ---
        # Edit the existing message instead of deleting and sending a new one
        await send_results_page(
            query.message.chat.id,
            final_results,
            page,
            context,
            search_query,
            message_id=query.message.message_id, # Pass the ID of the message to be edited
            new_message=False # Explicitly state this is an edit
        )

    elif data == "start_about":
        await query.message.delete()
        info_message = (
            "**About this Bot**\n\n"
            "This bot helps you find and share files on Telegram.\n"
            "• Developed by Kaustav Ray."
        )
        await query.message.reply_text(info_message, parse_mode="Markdown")

    elif data == "start_help":
        await query.message.delete()
        help_message = (
            "Here is a list of available commands optimized for this server:\n\n"
            "• /start - 𝑇𝑜 𝑠𝑡𝑎𝑟𝑡 𝑡ℎ𝑒 𝑏𝑜𝑡\n"
            "• /setskip - 𝑇𝑜 𝑠𝑘𝑖𝑝 𝑛𝑢𝑚𝑏𝑒𝑟 𝑜𝑓 𝑚𝑒𝑠𝑠𝑎𝑔𝑒𝑠 𝑤ℎ𝑒𝑛 𝑖𝑛𝑑𝑒𝑥𝑖𝑛𝑔 𝑓𝑖𝑙𝑒𝑠\n"
            "• /logs - 𝑡𝑜 𝑔𝑒𝑡 𝑡ℎ𝑒 𝑟𝑒𝑐𝑒𝑛𝑡 𝑒𝑟𝑟𝑜𝑟𝑠\n"
            "• /stats - 𝑡𝑜 𝑔𝑒𝑡 𝑠𝑡𝑎𝑡𝑢𝑠 𝑜𝑓 𝑓𝑖𝑙𝑒𝑠 𝑖𝑛 𝑑𝑏.\n"
            "• /connections - 𝑇𝑜 𝑠𝑒𝑒 𝑎𝑙𝑙 𝑐𝑜𝑛𝑛𝑒𝑐𝑡𝑒𝑑 𝑔𝑟𝑜𝑢𝑝𝑠\n"
            "• /settings - 𝑇𝑜 𝑜𝑝𝑒𝑛 𝑠𝑒𝑡𝑡𝑖𝑛𝑔𝑠 𝑚𝑒𝑛𝑢\n"
            "• /filter - 𝑎𝑑𝑑 𝑚𝑎𝑛𝑢𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠\n"
            "• /filters - 𝑣𝑖𝑒𝑤 𝑓𝑖𝑙𝑡𝑒𝑟𝑠\n"
            "• /connect - 𝑐𝑜𝑛𝑛𝑒𝑐𝑡 𝑡𝑜 𝑃𝑀.\n"
            "• /disconnect - 𝑑𝑖𝑠𝑐𝑜𝑛𝑛𝑒𝑐𝑡 𝑓𝑟𝑜𝑚 𝑃𝑀\n"
            "• /del - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎 𝑓𝑖𝑙𝑡𝑒𝑟\n"
            "• /delall - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎𝑙𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠\n"
            "• /deleteall - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎𝑙𝑙 𝑖𝑛𝑑𝑒𝑥𝑒𝑑 𝑓𝑖𝑙𝑒𝑠.\n"
            "• /delete - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎 𝑠𝑝𝑒𝑐𝑖𝑓𝑖𝑐 𝑓𝑖𝑙𝑒 𝑓𝑟𝑜𝑚 𝑖𝑛𝑑𝑒𝑥.\n"
            "• /info - 𝑔𝑒𝑡 𝑢𝑠𝑒𝑟 𝑖𝑛𝑓𝑜\n"
            "• /id - 𝑔𝑒𝑡 𝑡𝑔 𝑖𝑑𝑠.\n"
            "• /imdb - 𝑓𝑒𝑡𝑐ℎ 𝑖𝑛𝑓𝑜 𝑓𝑟𝑜𝑚 𝑖𝑚𝑑𝑏.\n"
            "• /users - 𝑡𝑜 𝑔𝑒𝑡 𝑙𝑖𝑠𝑡 𝑜𝑓 𝑚𝑦 𝑢𝑠𝑒𝑟𝑠 𝑎𝑛𝑑 𝑖𝑑𝑠.\n"
            "• /chats - 𝑡𝑜 𝑔𝑒𝑡 𝑙𝑖𝑠𝑡 𝑜𝑓 𝑡ℎ𝑒 𝑚𝑦 𝑐ℎ𝑎𝑡𝑠 𝑎𝑛𝑑 𝑖𝑑𝑠 \n"
            "• /leave  - 𝑡𝑜 𝑙𝑒𝑎𝑣𝑒 𝑓𝑟𝑜𝑚 𝑎 𝑐ℎ𝑎𝑡.\n"
            "• /disable  -  𝑑𝑜 𝑑𝑖𝑠𝑎𝑏𝑙𝑒 𝑎 𝑐ℎ𝑎𝑡.\n"
            "• /enable - 𝑟𝑒-𝑒𝑛𝑎𝑏𝑙𝑒 𝑐ℎ𝑎𝑡.\n"
            "• /ban  - 𝑡𝑜 𝑏𝑎𝑛 𝑎 𝑢𝑠𝑒𝑟.\n"
            "• /unban  - 𝑡𝑜 𝑢𝑛𝑏𝑎𝑛 𝑎 𝑢𝑠𝑒𝑟.\n"
            "• /channel - 𝑡𝑜 𝑔𝑒𝑡 𝑙𝑖𝑠𝑡 𝑜𝑓 𝑡𝑜𝑡𝑎𝑙 𝑐𝑜𝑛𝑛𝑒𝑐𝑡𝑒𝑑 𝑐ℎ𝑎𝑛𝑛𝑒𝑙𝑠\n"
            "• /broadcast - 𝑡𝑜 𝑏𝑟𝑜𝑎𝑑𝑐𝑎𝑠𝑡 𝑎 𝑚𝑒𝑠𝑠𝑎𝑔𝑒 𝑡𝑜 𝑎𝑙𝑙 𝑢𝑠𝑒𝑟𝑠\n"
            "• /grp_broadcast - 𝑇𝑜 𝑏𝑟𝑜𝑎𝑑𝑐𝑎𝑠𝑡 𝑎 𝑚𝑒𝑠𝑠𝑎𝑔𝑒 𝑡𝑜 𝑎𝑙𝑙 𝑐𝑜𝑛𝑛𝑒𝑐𝑡𝑒𝑑 𝑔𝑟𝑜𝑢𝑝𝑠.\n"
            "• /batch - 𝑡𝑜 𝑐𝑟𝑒𝑎𝑡𝑒 𝑙𝑖𝑛𝑘 𝑓𝑜𝑟 𝑚𝑢𝑙𝑡𝑖𝑝𝑙𝑒 𝑝𝑜𝑠𝑡𝑠\n"
            "• /link - 𝑡𝑜 𝑐𝑟𝑒𝑎𝑡𝑒 𝑙𝑖𝑛𝑘 𝑓𝑜𝑟 𝑜𝑛𝑒 𝑝𝑜𝑠𝑡\n"
            "• /set_template - 𝑇𝑜 𝑠𝑒𝑡 𝑎 𝑐𝑢𝑠𝑡𝑜𝑚 𝐼𝑀𝐷𝑏 𝑡𝑒𝑚𝑝𝑙𝑎𝑡𝑒 𝑓𝑜𝑟 𝑖𝑛𝑑𝑖𝑣𝑖𝑑𝑢𝑎𝑙 𝑔𝑟𝑜𝑢𝑝𝑠\n"
            "• /gfilter - 𝑇𝑜 𝑎𝑑𝑑 𝑔𝑙𝑜𝑏𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠.\n"
            "• /gfilters - 𝑇𝑜 𝑣𝑖𝑒𝑤 𝑙𝑖𝑠𝑡 𝑜𝑓 𝑎𝑙𝑙 𝑔𝑙𝑜𝑏𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠.\n"
            "• /delg - 𝑇𝑜 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎 𝑠𝑝𝑒𝑐𝑖𝑓𝑖𝑐 𝑔𝑙𝑜𝑏𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟.\n"
            "• /delallg - 𝑇𝑜 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎𝑙𝑙 𝑔𝑙𝑜𝑏𝑎𝑙 𝑓𝑖𝑙𝑡𝑒𝑟𝑠 𝑓𝑟𝑜𝑚 𝑡ℎ𝑒 𝑏𝑜𝑡'𝑠 𝑑𝑎𝑡𝑎𝑏𝑎𝑠𝑒.\n"
            "• /deletefiles - 𝑇𝑜 𝑑𝑒𝑙𝑒𝑡𝑒 𝑃𝑟𝑒𝐷𝑉𝐷 𝑎𝑛𝑑 𝐶𝑎𝑚𝑅𝑖𝑝 𝐹𝑖𝑙𝑒𝑠 𝑓𝑟𝑜𝑚 𝑡ℎ𝑒 𝑏𝑜𝑡'𝑠 𝑑𝑎𝑡𝑎𝑏𝑎𝑠𝑒.\n"
            "• /add_premium - 𝐴𝑑𝑑 𝑢𝑠𝑒𝑟 𝑡𝑜 𝑝𝑟𝑒𝑚𝑖𝑢𝑚 𝑙𝑖𝑠𝑡\n"
            "• /remove_premium - 𝑅𝑒𝑚𝑜𝑣𝑒 𝑢𝑠𝑒𝑟 𝑓𝑟𝑜𝑚 𝑝𝑟𝑒𝑚𝑖𝑢𝑚 𝑙𝑖𝑠𝑡\n"
            "• /plan - 𝐶ℎ𝑒𝑐𝑘 𝑝𝑙𝑎𝑛 𝑑𝑒𝑡𝑎𝑖𝑙𝑠\n"
            "• /myplan - 𝐶ℎ𝑒𝑐𝑘 𝑦𝑜𝑢𝑟 𝑝𝑙𝑎𝑛 𝑠𝑡𝑎𝑡𝑠\n"
            "• /shortlink - 𝑠𝑒𝑡 𝑦𝑜𝑢𝑟 𝑢𝑟𝑙 𝑠ℎ𝑜𝑟𝑡𝑛𝑒𝑟 𝑖𝑛 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝\n"
            "• /setshortlinkoff  - 𝑜𝑓𝑓 𝑠ℎ𝑜𝑟𝑡𝑙𝑖𝑛𝑘 𝑖𝑛 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝\n"
            "• /setshortlinkon - 𝑜𝑛 𝑠ℎ𝑜𝑟𝑡𝑙𝑖𝑛𝑘 𝑖𝑛 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝\n"
            "• /shortlink_info - 𝑐ℎ𝑒𝑐𝑘 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝 𝑎𝑙𝑙 𝑠ℎ𝑜𝑟𝑡𝑙𝑖𝑛𝑘 𝑎𝑛𝑑 𝑡𝑢𝑡𝑜𝑟𝑖𝑎𝑙 𝑙𝑖𝑛𝑘 𝑑𝑒𝑡𝑎𝑖𝑙𝑠\n"
            "• /set_tutorial - 𝑠𝑒𝑡 𝑦𝑜𝑢𝑟 𝑢𝑟𝑙 𝑠ℎ𝑜𝑟𝑡𝑛𝑒𝑟 ℎ𝑜𝑤 𝑡𝑜 𝑜𝑝𝑒𝑛 𝑙𝑖𝑛𝑘 𝑢𝑟𝑙\n"
            "• /remove_tutorial - 𝑟𝑒𝑚𝑜𝑣𝑒 𝑦𝑜𝑢𝑟 𝑡𝑢𝑡𝑜𝑟𝑖𝑎𝑙 𝑢𝑟𝑙\n"
            "• /fsub - 𝑎𝑑𝑑 𝑓𝑜𝑟𝑐𝑒 𝑠𝑢𝑏𝑠𝑐𝑟𝑖𝑏𝑒 𝑐ℎ𝑎𝑛𝑛𝑒𝑙 𝑖𝑛 𝑔𝑟𝑜𝑢𝑝\n"
            "• /nofsub - 𝑟𝑒𝑚𝑜𝑣𝑒 𝑜𝑟 𝑜𝑓𝑓 𝑓𝑜𝑟𝑐𝑒 𝑠𝑢𝑏𝑠𝑐𝑟𝑖𝑏𝑒 𝑖𝑛 𝑦𝑜𝑢𝑟 𝑔𝑟𝑜𝑢𝑝\n"
            "• /rename - 𝑟𝑒𝑛𝑎𝑚𝑒 𝑦𝑜𝑢𝑟 𝑓𝑖𝑙𝑒\n"
            "• /set_caption - 𝑎𝑑𝑑 𝑐𝑎𝑝𝑡𝑖𝑜𝑛 𝑓𝑜𝑟 𝑦𝑜𝑢𝑟 𝑟𝑒𝑛𝑎𝑚𝑒𝑑 𝑓𝑖𝑙𝑒\n"
            "• /see_caption - 𝑠𝑒𝑒 𝑦𝑜𝑢𝑟 𝑠𝑎𝑣𝑒𝑑 𝑐𝑎𝑝𝑡𝑖𝑜𝑛\n"
            "• /del_caption - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑦𝑜𝑢𝑟 𝑠𝑎𝑣𝑒𝑑 𝑐𝑎𝑝𝑡𝑖𝑜𝑛\n"
            "• /set_thumb - 𝑎𝑑𝑑 𝑡ℎ𝑢𝑚𝑏𝑛𝑎𝑖𝑙 𝑓𝑜𝑟 𝑦𝑜𝑢𝑟 𝑟𝑒𝑛𝑎𝑚𝑒𝑑 𝑓𝑖𝑙𝑒\n"
            "• /view_thumb - 𝑣𝑖𝑒𝑤 𝑦𝑜𝑢𝑟 𝑠𝑎𝑣𝑒𝑑 𝑡ℎ𝑢𝑚𝑏𝑛𝑎𝑖𝑙\n"
            "• /del_thumb - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑦𝑜𝑢𝑟 𝑠𝑎𝑣𝑒𝑑 𝑡ℎ𝑢𝑚𝑏𝑛𝑎𝑖𝑙\n"
            "• /stream - 𝑔𝑒𝑛𝑒𝑟𝑎𝑡𝑒 𝑠𝑡𝑟𝑒𝑎𝑚 𝑎𝑛𝑑 𝑑𝑜𝑤𝑛𝑙𝑜𝑎𝑑 𝑙𝑖nk 𝑜𝑓 𝑦𝑜𝑢𝑟 𝑓𝑖𝑙𝑒\n"
            "• /telegraph - 𝑔𝑒𝑡 𝑡𝑒𝑙𝑒𝑔𝑟𝑎𝑝ℎ 𝑙𝑖𝑛𝑘 𝑜𝑓 𝑎𝑛𝑦 𝑓𝑖𝑙𝑒 𝑢𝑛𝑑𝑒𝑟 5𝑚𝑏\n"
            "• /stickerid - 𝑡𝑜 𝑔𝑒𝑡 𝑖𝑑 𝑎𝑛𝑑 𝑢𝑛𝑖𝑞𝑢𝑒 𝐼'𝑑 𝑜𝑓 𝑠𝑡𝑖𝑐𝑘𝑒𝑟\n"
            "• /font - 𝑡𝑜 𝑔𝑒𝑡 𝑎𝑛𝑦 𝑡𝑦𝑝𝑒 𝑜𝑓 𝑓𝑜𝑛𝑡 𝑜𝑓 𝑎𝑛𝑦 𝑤𝑜𝑟𝑑\n"
            "• /repo - 𝑔𝑒𝑡 𝑎𝑛𝑦 𝑟𝑒𝑝𝑜 𝑙𝑖𝑛𝑘 𝑏𝑦 𝑠𝑒𝑎𝑟𝑐ℎ𝑖𝑛𝑔\n"
            "• /purgerequests - 𝑑𝑒𝑙𝑒𝑡𝑒 𝑎𝑙𝑙 𝑗𝑜𝑖𝑛 𝑟𝑒𝑞𝑢𝑒𝑠𝑡𝑠 𝑓𝑟𝑜𝑚 𝑑𝑎𝑡𝑎𝑏𝑎𝑠𝑒\n"
            "• /totalrequests - 𝑔𝑒𝑡 𝑡𝑜𝑡𝑎𝑙 𝑛𝑢𝑚𝑏𝑒𝑟 𝑜𝑓 𝑗𝑜𝑖𝑛 𝑟𝑒𝑞𝑢𝑒𝑠𝑡 𝑓𝑟𝑜𝑚 𝑑𝑎𝑡𝑎𝑏𝑎𝑠𝑒\n"
        )
        await query.message.reply_text(help_message)

    elif data == "start_close":
        await query.message.delete()
    elif data == "no_owner":
        await query.answer("Owner not configured.", show_alert=True)

    elif data.startswith("sendall_"):

        # Update the user message to reflect that only the current page is being sent
        await query.message.reply_text("📨 Sending all files on the **current page**. Please check your private chat with me. This may take a moment...")

        _, page_str, search_query = data.split("_", 2)
        page = int(page_str)

        # Retrieve search results from user_data
        final_results = context.user_data.get('search_results')

        if not final_results:
            # Re-run search logic (same as in 'page_' section for robustness)
            normalized_query = search_query.replace("_", " ").replace(".", " ").replace("-", " ").strip()
            words = [re.escape(word) for word in normalized_query.split() if len(word) > 1]
            if not words:
                await query.message.reply_text("❌ Query too short or invalid for recovery.")
                return

            regex_pattern = re.compile("|".join(words), re.IGNORECASE)
            query_filter = {"file_name": {"$regex": regex_pattern}}
            preliminary_results = []

            for uri in MONGO_URIS:
                temp_client = None
                try:
                    temp_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                    temp_client.admin.command('ismaster')
                    temp_db = temp_client["telegram_files"]
                    temp_files_col = temp_db["files"]
                    results = list(temp_files_col.find(query_filter))
                    preliminary_results.extend(results)
                except Exception as e:
                    logger.error(f"MongoDB search query failed during recovery for sendall: {e}")
                finally:
                    if temp_client:
                        temp_client.close()

            results_with_score = []
            unique_files = set()
            for file in preliminary_results:
                file_key = (file.get('file_id'), file.get('channel_id'))
                if file_key in unique_files: continue
                score = fuzz.token_set_ratio(normalized_query, file['file_name'])
                if score > 40:
                    results_with_score.append((file, score))
                    unique_files.add(file_key)

            sorted_results = sorted(results_with_score, key=lambda x: x[1], reverse=True)
            final_results = [result[0] for result in sorted_results[:50]]

            if not final_results:
                await query.message.reply_text("❌ No relevant files found after recovery for sendall.")
                return

            # Save recovered results
            context.user_data['search_results'] = final_results
            context.user_data['search_query'] = search_query


        # Get the files only for the current page
        files_to_send = final_results[page * 10:(page + 1) * 10]

        if not files_to_send:
            await query.message.reply_text("❌ No files found on this page to send.")
            return

        # Schedule the batch sending task
        asyncio.create_task(send_all_files_task(query, context, files_to_send))


# ========================
# MAIN
# ========================

def main():
    if not connect_to_mongo():
        logger.critical("Failed to connect to the initial MongoDB URI. Exiting.")
        return

    app = Application.builder().token(BOT_TOKEN).build()

    # Command Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("log", log_command))
    app.add_handler(CommandHandler("total_users", total_users_command))
    app.add_handler(CommandHandler("total_files", total_files_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("deletefile", delete_file_command))
    app.add_handler(CommandHandler("findfile", find_file_command))
    app.add_handler(CommandHandler("deleteall", delete_all_command))
    app.add_handler(CommandHandler("ban", ban_user_command))
    app.add_handler(CommandHandler("unban", unban_user_command))
    app.add_handler(CommandHandler("broadcast", broadcast_message))

    # File and Message Handlers
    # Admin file upload via PM
    app.add_handler(MessageHandler(
        (filters.Document.ALL | filters.VIDEO | filters.AUDIO) & filters.ChatType.PRIVATE,
        save_file_from_pm
    ))

    # Admin file indexing via DB Channel
    app.add_handler(MessageHandler(
        (filters.Document.ALL | filters.VIDEO | filters.AUDIO) & filters.Chat(chat_id=DB_CHANNEL),
        save_file_from_channel
    ))

    # Text Search Handler (REVISED LOGIC)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_files))

    # Callback Query Handler (for buttons)
    app.add_handler(CallbackQueryHandler(button_handler))

    logger.info("Bot started...")
    # Start the bot
    app.run_polling(poll_interval=1, timeout=10, drop_pending_updates=True)


if __name__ == "__main__":
    main()