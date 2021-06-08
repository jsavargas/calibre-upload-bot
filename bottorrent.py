#!/usr/bin/env python3
'''
BASADO EN EL BOT DE DekkaR - 2021
'''
VERSION = """
VERSION 1.10
"""
HELP = """
/help		: Esta pantalla.
/autor AUTOR : search autor  
/title TITLE : search books by title  
/serie SERIE : search series  
/all TITLE OR AUTOR : search books by autor or title  
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
import subprocess 

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
TG_CONVERTS_BOOKS = get_env('TG_CONVERTS_BOOKS', 'True')


usuarios = list(map(int, TG_AUTHORIZED_USER_ID.replace(" ", "").split(','))) if TG_AUTHORIZED_USER_ID else False 


queue = asyncio.Queue()
number_of_parallel_downloads = int(os.environ.get('TG_MAX_PARALLEL',4))
maximum_seconds_per_download = int(os.environ.get('TG_DL_TIMEOUT',3600))

temp_completed_path = ''


con = sqlite3.connect(os.path.join(TG_BOOKS_PATH,'metadata.db'))

async def getBooksbyID(update,con,id):
	msg = await update.reply('Enviando...')
	real_id = get_peer_id(update.message.peer_id)
	CID , peer_type = resolve_id(real_id)
	
	cursorObj = con.cursor() 
	#cursorObj.execute('SELECT id,title,author_sort,path FROM books WHERE id = "{}"'.format(id))

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format, books.has_cover
							from books
							INNER JOIN data	
							ON books.id = data.book
							where books.id = {} 
							order by books.author_sort,books.title limit 1'''.format(id))

	for row in cursorObj.fetchall():
		id,author_sort,title,path,name,format,has_cover = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		if has_cover: cover = os.path.join(TG_BOOKS_PATH,path, '{}'.format('cover.jpg'))
		if os.path.exists(file):
			await msg.edit('Enviando archivo...')
			loop = asyncio.get_event_loop()
			if os.path.exists(cover):
				await client.send_file(CID, cover)
			task = loop.create_task(tg_send_file(CID,file,name))
			download_result = await asyncio.wait_for(task, timeout = maximum_seconds_per_download)
			mobi = os.path.join('/output', '{}.{}'.format(name,'mobi'))
			if eval(TG_CONVERTS_BOOKS) and format.lower() != 'mobi':
				logger.info("CONVERT TO MOBI: ")
				msgmobi = await update.reply('Convirtiendo a mobi...')
				process = subprocess.Popen(["ebook-convert",file,mobi], stdout=subprocess.PIPE, universal_newlines=True)
				# Poll process for new output until finished
				while True:
					nextline = process.stdout.readline()
					if nextline == '' and process.poll() is not None:
						break
					sys.stdout.write(nextline)
					sys.stdout.flush()

				output = process.communicate()[0]
				exitCode = process.returncode

				logger.info("MOBI: {}".format(mobi))
				if os.path.exists(mobi):
					await msgmobi.edit("Enviando archivo mobi...")
					task = loop.create_task(tg_send_file(CID,mobi,name))
					download_result = await asyncio.wait_for(task, timeout = maximum_seconds_per_download)
				await msg.edit('Archivos enviados...')
				time.sleep(1)
				await msg.delete()
				await msgmobi.delete()


async def getBooksAll(update,con,title):

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
	msg = await update.reply('Buscando...')

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
							from books
							INNER JOIN data	
							ON books.id = data.book
							where data.name LIKE '%{}%' 
							order by books.author_sort,books.title limit 30'''.format(title))


	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} resultados....'.format(len(rows)))
	time.sleep(1)
	temp = ''
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		temp += "[{sending}] \U0001F4DA {name} /bm{id} \n".format(
														sending=sending,name=name,id=id)

		_buttons.append([Button.inline("\U0001F4DA {}".format(name), id)])
	if rows:
		await msg.edit("Seleccione un libro para descargar:\n" + temp)
	else: await msg.edit('No se encontraron resultados')

async def getAuthors(update,con,title):

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
	msg = await update.reply('Buscando...')

	cursorObj.execute('''select authors.id, authors.name, authors.sort ,count(authors.id) as count from authors
						INNER JOIN books_authors_link
						ON books_authors_link.author = authors.id
						where authors.sort LIKE '%{}%' 
						group by authors.id
						limit 30
						'''.format(title))


	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} resultados....'.format(len(rows)))
	time.sleep(1)
	temp = ''
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id, name, sort, count = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		temp += "[{sending}] \U0001F4DA {sort} ({count}) /ax{id} \n".format(
						sending=sending,count=count, sort=sort,id=id)

	if rows:
		await msg.edit("Seleccione un Autor:\n" + temp)
	else: await msg.edit('No se encontraron resultados')

async def getBooksTitle(update,con,title):

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
	msg = await update.reply('Buscando...')

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
							from books
							INNER JOIN data	
							ON books.id = data.book
							where books.title LIKE '%{}%' 
							order by books.author_sort,books.title limit 30'''.format(title))

	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} Resultados....'.format(len(rows)))
	
	temp = ''
	time.sleep(1)
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		temp += "[{}] \U0001F4DA {} /bm{} \n".format(sending,name,id)

	if rows:
		await msg.edit("Seleccione un libro para descargar:\n" + temp)
	else: await msg.edit('No se encontraron resultados')
       
async def getSeries(update,con,title):

	real_id = get_peer_id(update.message.peer_id)
	CID , peer_type = resolve_id(real_id)
	from_id = ''
	if update.message.from_id is not None:
		from_id = update.message.from_id.user_id

	logger.info(title)
	cursorObj = con.cursor()
	msg = await update.reply('Buscando Series...')

	cursorObj.execute('''select series.id, series.name, series.sort from series
							where series.sort LIKE '%{}%' 
						limit 30 '''.format(title))


	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} resultados....'.format(len(rows)))
	time.sleep(1)
	temp = ''
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id, name, sort = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		temp += "[{}] \U0001F4DA {} /se{} \n".format(sending, sort,id)

	if rows:
		await msg.edit("Seleccione una Serie:\n" + temp)
	else: await msg.edit('No se encontraron resultados')

async def getBooksbyAutor(update,con,id):

	real_id = get_peer_id(update.message.peer_id)
	CID , peer_type = resolve_id(real_id)
	from_id = ''
	if update.message.from_id is not None:
		from_id = update.message.from_id.user_id
		#logger.info("USER ON GROUP => U:[%s]G:[%s]M:[%s]" % (update.message.from_id.user_id,CID,update.message.message))

	#my_user    = await client.get_entity(types.PeerUser(CID))
	#logger.info(my_user.username)

	logger.info(id)
	cursorObj = con.cursor()
	msg = await update.reply('Buscando...')

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
							from books
							INNER JOIN data	
							ON books.id = data.book
							INNER JOIN books_authors_link
							ON books_authors_link.book = books.id
							where books_authors_link.author = {}
							order by books.author_sort,books.title 						
					'''.format(id))

	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} Resultados....'.format(len(rows)))
	
	temp = ''
	time.sleep(1)
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		temp += "[{}] \U0001F4DA {} /bm{} \n".format(sending, name,id)

		_buttons.append([Button.inline("\U0001F4DA {}".format(name), id)])
	if rows:
		await msg.edit("Seleccione un libro para descargar:\n" + temp)
	else: await msg.edit('No se encontraron resultados')

async def getBooksbySeries(update,con,id):

	real_id = get_peer_id(update.message.peer_id)
	CID , peer_type = resolve_id(real_id)
	from_id = ''
	if update.message.from_id is not None:
		from_id = update.message.from_id.user_id
		#logger.info("USER ON GROUP => U:[%s]G:[%s]M:[%s]" % (update.message.from_id.user_id,CID,update.message.message))

	#my_user    = await client.get_entity(types.PeerUser(CID))
	#logger.info(my_user.username)

	logger.info(id)
	cursorObj = con.cursor()
	msg = await update.reply('Buscando...')

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format,books.series_index
							from books
							INNER JOIN data	
							ON books.id = data.book
							INNER JOIN books_series_link
							ON books_series_link.book = books.id
							where books_series_link.series = {}
							order by books.author_sort,books.series_index,books.title 		 						
					'''.format(id))

	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} Resultados....'.format(len(rows)))
	
	temp = ''
	time.sleep(1)
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format,series_index = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		temp += "[{sending}] \U0001F4DA ({series_index}) {name} /bm{id} \n".format(
												sending=sending, name=name,series_index=series_index,id=id)
	if rows:
		await msg.edit("Seleccione un Libro para descargar:\n" + temp)
	else: await msg.edit('No se encontraron resultados')

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


@events.register(events.NewMessage)
async def handler(update):
	global temp_completed_path
	global FOLDER_GROUP
 
	logger.info("NewMessage[%s]" % (update.message.message))
 
	try:

		real_id = get_peer_id(update.message.peer_id)
		CID , peer_type = resolve_id(real_id)

		if update.message.from_id is not None:
			logger.info("USER ON GROUP => U:[%s]G:[%s]M:[%s]" % (update.message.from_id.user_id,CID,update.message.message))


		if not TG_AUTHORIZED_USER_ID or CID in usuarios:
			if update.message.message == '/help':
				message = await update.reply(HELP) 
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
			elif ((update.message.message).startswith('/all')):
				msg = update.message.message
				logger.info("SEND BOOKS :[%s]",msg)
				rest = await getBooksAll(update,con,msg.replace('/all ',''))
				logger.info("FINISH SEND BOOKS :[%s]",msg)
			elif ((update.message.message).startswith('/title')):
				msg = update.message.message
				logger.info("SEND BOOKS :[%s]",msg)
				rest = await getBooksTitle(update,con,msg.replace('/title ',''))
				logger.info("FINISH SEND BOOKS :[%s]",msg)
			elif ((update.message.message).startswith('/autor')):
				msg = update.message.message
				logger.info("SEND BOOKS :[%s]",msg)
				rest = await getAuthors(update,con,msg.replace('/autor ',''))
				logger.info("FINISH SEND BOOKS :[%s]",msg)
			elif ((update.message.message).startswith('/serie')):
				msg = update.message.message
				logger.info("SEND SERIES :[%s]",msg)
				rest = await getSeries(update,con,msg.replace('/serie ',''))
			elif ((update.message.message).startswith('/bm')):
				msg = update.message.message
				m = re.search('/bm(.+?)(?=@).*', msg)
				if m:
					rest = await getBooksbyID(update,con,m.group(1))
				else:
					rest = await getBooksbyID(update,con,msg.replace('/bm',''))
			elif ((update.message.message).startswith('/ax')):
				msg = update.message.message
				m = re.search('/ax(.+?)(?=@).*', msg)
				if m:
					rest = await getBooksbyAutor(update,con,m.group(1))
				else:
					rest = await getBooksbyAutor(update,con,msg.replace('/ax',''))
			elif ((update.message.message).startswith('/se')):
				msg = update.message.message
				m = re.search('/se(.+?)(?=@).*', msg)
				if m:
					rest = await getBooksbySeries(update,con,m.group(1))
				else:
					rest = await getBooksbySeries(update,con,msg.replace('/se',''))
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
	

