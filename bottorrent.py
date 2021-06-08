#!/usr/bin/env python3
'''
BASADO EN EL BOT DE DekkaR - 2021
'''
VERSION = """
VERSION 1.2
"""
REQUIREMENTS = """
 Instalar telethon	 --> sudo python3 -m pip install telethon
 Instalar cryptg	 --> sudo python3 -m pip install cryptg
"""
LICENCIA = '''
'''
HELP = """
/help		: Esta pantalla.
/alive		: keep-alive.
/version	: Version.  
/me			: ID TELEGRAM y mas informacion en el log  
/title TITLE : search books by title  
"""
UPDATE = """BASADO EN EL BOT DE @DekkaR - 2021:
- UPLOAD BOOKS IN CALIBRE LIBRARY DB ('metadata.db') /books => ../Calibre Library:/books
"""

import re
import os
import shutil
import sys
import time
import asyncio
import cryptg
# Imports Telethon
from telethon import TelegramClient, Button, events
from telethon.tl import types
from telethon.utils import get_extension, get_peer_id, resolve_id
import sqlite3
import json
import logging

'''
LOGGER
'''

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '[%(asctime)s] %(levelname)-7s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# Variables de cada usuario ######################
# This is a helper method to access environment variables or
# prompt the user to type them in the terminal if missing.
def get_env(name, message, cast=str):
	if name in os.environ:
		logger.info('%s: %s' % (name , os.environ[name]))
		return os.environ[name]
	else:
		logger.info('%s: %s' % (name , message))
		return message


# Define some variables so the code reads easier
session = os.environ.get('TG_SESSION', 'bottorrent')
api_id = get_env('TG_API_ID', 'Enter your API ID: ', int)
api_hash = get_env('TG_API_HASH', 'Enter your API hash: ')
bot_token = get_env('TG_BOT_TOKEN', 'Enter your Telegram BOT token: ')
TG_AUTHORIZED_USER_ID = get_env('TG_AUTHORIZED_USER_ID', False)
TG_BOOKS_PATH = get_env('TG_DOWNLOAD_PATH', '/books')


usuarios = list(map(int, TG_AUTHORIZED_USER_ID.replace(" ", "").split(','))) if TG_AUTHORIZED_USER_ID else False 


queue = asyncio.Queue()
number_of_parallel_downloads = int(os.environ.get('TG_MAX_PARALLEL',4))
maximum_seconds_per_download = int(os.environ.get('TG_DL_TIMEOUT',3600))

temp_completed_path = ''


con = sqlite3.connect(os.path.join(TG_BOOKS_PATH,'metadata.db'))

async def getBooksAuthor(update,con,title):

	real_id = get_peer_id(update.message.peer_id)
	CID , peer_type = resolve_id(real_id)
	from_id = ''
	if update.message.from_id is not None:
		from_id = update.message.from_id.user_id
		#logger.info("USER ON GROUP => U:[%s]G:[%s]M:[%s]" % (update.message.from_id.user_id,CID,update.message.message))

	#my_user    = await client.get_entity(types.PeerUser(CID))
	#logger.info(my_user.username)

	logger.info(title)
	cursorObj = con.cursor()

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
							from books
							INNER JOIN data	
							ON books.id = data.book
							where books.author_sort LIKE '%{}%' 
							order by books.author_sort,books.title limit 20'''.format(title))


	rows = cursorObj.fetchall()

	if rows: msg = await update.reply('Enviando {} resultados....'.format(len(rows)))
	time.sleep(1)
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))

		_buttons.append([Button.inline("\U0001F4DA {}".format(name), id)])

	if rows:
		msg = await client.send_message(CID, "Seleccione un libro para descargar:", buttons=_buttons)
       

async def getBooks(update,con,title):

	real_id = get_peer_id(update.message.peer_id)
	CID , peer_type = resolve_id(real_id)
	from_id = ''
	if update.message.from_id is not None:
		from_id = update.message.from_id.user_id
		#logger.info("USER ON GROUP => U:[%s]G:[%s]M:[%s]" % (update.message.from_id.user_id,CID,update.message.message))

	#my_user    = await client.get_entity(types.PeerUser(CID))
	#logger.info(my_user.username)

	logger.info(title)
	cursorObj = con.cursor()

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
							from books
							INNER JOIN data	
							ON books.id = data.book
							where books.title LIKE '%{}%' 
							order by books.author_sort,books.title limit 20'''.format(title))

	rows = cursorObj.fetchall()

	if rows: msg = await update.reply('Enviando {} Resultados....'.format(len(rows)))
	time.sleep(1)
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))

		_buttons.append([Button.inline("\U0001F4DA {}".format(name), id)])
	if rows:
		msg = await client.send_message(CID, "Seleccione un libro para descargar:", buttons=_buttons)
       

async def tg_send_message(msg):
	try:
		if TG_AUTHORIZED_USER_ID: await client.send_message(usuarios[0], msg)
	except Exception as e:
		logger.info('Exception: %s ', str(e))
 
	return True

async def tg_send_file(CID,file,name=''):
    #await client.send_file(6537360, file)
    async with client.action(CID, 'document') as action:
    	await client.send_file(CID, file,caption=name,force_document=True,progress_callback=action.progress)
	#await client.send_message(6537360, file)


client = TelegramClient(session, api_id, api_hash, proxy = None, request_retries = 10, flood_sleep_threshold = 120)

@client.on(events.CallbackQuery)
async def handler(event):
	id = (event.data).decode(encoding="utf-8")
	#logger.info(" ID [%s]",id)
		
	await event.answer('You clicked {}!'.format(id))
	
	cursorObj = con.cursor() 
	cursorObj.execute('SELECT id,title,author_sort,path FROM books WHERE id = "{}"'.format(id))

	for row in cursorObj.fetchall():
		id,title,author_sort,path = row
		cursorObj.execute('SELECT id,book,format,name FROM data WHERE book = {}'.format((id)))

		for data in cursorObj.fetchall():
			id,book,format,name = data
			file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
			if os.path.exists(file):
				logger.info("DATA:::: [{}][{}][{}]".format(id,name,file))
				loop = asyncio.get_event_loop()
				task = loop.create_task(tg_send_file(event.chat_id,file,name))
				download_result = await asyncio.wait_for(task, timeout = maximum_seconds_per_download)



@events.register(events.NewMessage)
async def handler(update):
	global temp_completed_path
	global FOLDER_GROUP
 
	logger.info("events.NewMessage[%s]" % (update.message.message))
 
	try:

		real_id = get_peer_id(update.message.peer_id)
		CID , peer_type = resolve_id(real_id)

		if update.message.from_id is not None:
			logger.info("USER ON GROUP => U:[%s]G:[%s]M:[%s]" % (update.message.from_id.user_id,CID,update.message.message))


		if not TG_AUTHORIZED_USER_ID or CID in usuarios:
			if update.message.message == '/help':
				message = await update.reply(HELP) 
				await queue.put([update, message])
			elif update.message.message == '/start': 
				message = await update.reply(LICENCIA)
				await queue.put([update, message])
			elif update.message.message == '/version': 
				message = await update.reply(VERSION)
				await queue.put([update, message,temp_completed_path])
			elif update.message.message == '/alive': 
				message = await update.reply('Keep-Alive')
				await queue.put([update, message,temp_completed_path])
			elif update.message.message == '/me': 
				message = await update.reply('me: your id is: {}'.format(CID) )
				await queue.put([update, message,temp_completed_path])
				logger.info('me :[%s] [%s]]' % (CID,update.message.message))
			elif ((update.message.message).startswith('/title')):
				msg = update.message.message
				logger.info("SEND BOOKS :[%s]",msg)
				rest = await getBooks(update,con,msg.replace('/title ',''))
				logger.info("FINISH SEND BOOKS :[%s]",msg)
			elif ((update.message.message).startswith('/autor')):
				msg = update.message.message
				logger.info("SEND BOOKS :[%s]",msg)
				rest = await getBooksAuthor(update,con,msg.replace('/autor ',''))
				logger.info("FINISH SEND BOOKS :[%s]",msg)
		else:
			logger.info('USUARIO: %s NO AUTORIZADO', CID)
			message = await update.reply('USUARIO: %s NO AUTORIZADO\n agregar este ID a TG_AUTHORIZED_USER_ID' % CID)
	except Exception as e:
		message = await update.reply('ERROR: ' + str(e))
		logger.info('Exception: %s ', str(e))

try:

	loop = asyncio.get_event_loop()
	# Arrancamos bot con token
	client.start(bot_token=str(bot_token))
	client.add_event_handler(handler)

	# Pulsa Ctrl+C para detener
	loop.run_until_complete(tg_send_message("Bot Books Upload Started"))
	logger.info("********** Bot Books Upload Started **********")

	client.run_until_disconnected()
finally:
	# Stop Telethon
	client.disconnect()
	print(' Parado!!! ')
	

