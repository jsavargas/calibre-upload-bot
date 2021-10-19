#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

VERSION = "VERSION 1.13.11"
HELP = """
Bienvenid@ 
Este bot cuenta con una biblioteca de más de 88 mil libros en epub los cuales son convertidos a mobi para poder enviarlos a nuestros kindles 

¿Como se usa este bot? Muy simple, puedes pedirle que busque por autor /autor y nombre del autor, apellido o nombre, ejemplo:
/autor Stephen King 
/autor Stephen 
/autor King 

También puedes buscar por título del libro o por una parte de este, ejemplo:
/title La historia de Lisey
/title Lisey
/title historia de Lisey

También puedes buscar por series o por una parte de esta, ejemplo:
/serie el señor de los anillos
/serie señor de los anillos
/serie anillos

También puedes buscar por autor y título del libro usando el comando /all, ejemplo:
/all Tolkien

Y también puedes buscar series por autor y te buscará todas las series de ese autor, ejemplo: 
/serieautor Stephen King
/serieautor Anne Rice
/serieautor Tolkien

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
from telethon import TelegramClient, events
from telethon.tl import types
from telethon.utils import get_extension, get_peer_id, resolve_id, split_text
from telethon.extensions import markdown
import sqlite3
import json
import logging
import subprocess 
import random
import threading

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

max_text = 1020

temp_completed_path = ''


con = sqlite3.connect(os.path.join(TG_BOOKS_PATH,'metadata.db'))

async def tg_send_message(msg):
    if TG_AUTHORIZED_USER_ID: await client.send_message(usuarios[0], msg)
    return True

async def tg_send_file(CID,file,name=''):
    #await client.send_file(6537360, file)
    async with client.action(CID, 'document') as action:
    	await client.send_file(CID, file,caption=name,force_document=True,progress_callback=action.progress)
	#await client.send_message(6537360, file)

async def CONVERTS_BOOKS(message,file,name):
	try:

		logger.info("init CONVERTS_BOOKS  {}".format(file))
		real_id = get_peer_id(message.peer_id)
		CID , peer_type = resolve_id(real_id)
		
		mobi = os.path.join('/output', '{}.{}'.format(name,'mobi'))

		if not os.path.exists(mobi):
			logger.info("CONVERT TO MOBI: ")
			await message.edit('Convirtiendo a mobi...')
			process = subprocess.Popen(["ebook-convert",file,mobi], stdout=subprocess.PIPE, universal_newlines=True)
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
				await message.edit("Enviando archivo mobi...")
				await tg_send_file(CID,mobi,name)
		else:
			await message.edit("Enviando archivo mobi...")
			await tg_send_file(CID,mobi,name)


	except Exception as e:
		logger.info('CONVERTS_BOOKS ERROR: %s Books Upload: %s' % (e.__class__.__name__, str(e)))

	logger.info("finish CONVERTS_BOOKS {}".format(file))


async def getBooksbyID(con,message,id):
	msg = await message.edit('Buscando...')
	real_id = get_peer_id(message.peer_id)
	CID , peer_type = resolve_id(real_id)
	
	logger.info("getBooksbyID[{}]".format(id))

	if id == '': 
		await msg.edit('No se encontraron resultados')
		return 


	cursorObj = con.cursor() 
	#cursorObj.execute('SELECT id,title,author_sort,path FROM books WHERE id = "{}"'.format(id))

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format, books.has_cover, comments.text
							from books
							INNER JOIN data	
							ON books.id = data.book
							INNER JOIN comments
							ON books.id = comments.book
							where books.id = {} and books.id = comments.book
							order by books.author_sort,books.title limit 1'''.format(id))

	for row in cursorObj.fetchall():
		id,author_sort,title,path,name,format,has_cover,text = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		if has_cover: cover = os.path.join(TG_BOOKS_PATH,path, '{}'.format('cover.jpg'))
		if os.path.exists(file):
			await msg.edit('Enviando archivo...')
			loop = asyncio.get_event_loop()
			if os.path.exists(cover):
				resena = "{}".format(text[:max_text] + "...") if len(text) > max_text else text
				await client.send_file(CID, cover, caption=resena)
				await tg_send_file(CID,file,name)
				mobi = os.path.join('/output', '{}.{}'.format(name,'mobi'))

			if eval(TG_CONVERTS_BOOKS) and format.lower() != 'mobi':
				await CONVERTS_BOOKS(message,file,name)
			
			await msg.edit('Archivos enviados...')

async def getBooksTitle(con,message,title):


	cursorObj = con.cursor()
	msg = await message.edit('Buscando...')

	
	if title != '/title': 
		cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
								from books
								INNER JOIN data	
								ON books.id = data.book
								where books.title LIKE '%{}%' 
								order by books.author_sort,books.title limit 30'''.format(title))
	else:
		cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
								from books
								INNER JOIN data	
								ON books.id = data.book
								ORDER BY RANDOM()
								limit 30''')

	__rows = cursorObj.fetchall()
	rows = sorted(__rows, key=lambda __rows: __rows[2])

	if rows: await msg.edit('Enviando {} Resultados....'.format(len(rows)))
	
	temp = ''
	
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
       
async def getAuthors(con,message,title):

	cursorObj = con.cursor()
	msg = await message.edit('Buscando...')

	logger.info("autor[{}]".format(title))

	if title != '/autor': 
		cursorObj.execute('''select authors.id, authors.name, authors.sort ,count(authors.id) as count from authors
							INNER JOIN books_authors_link
							ON books_authors_link.author = authors.id
							where authors.sort LIKE '%{}%' 
							group by authors.id
							limit 30
							'''.format(title))
	else:
		randomlist = random.sample(range(1, 30000), 100)
		random.shuffle(randomlist)
		converted_list = [str(element) for element in randomlist]
		joined_string = ",".join(converted_list)

		sql = '''select authors.id, authors.name, authors.sort ,count(authors.id) as count from authors
							INNER JOIN books_authors_link
							ON books_authors_link.author = authors.id
							where authors.id in ({})
							group by authors.id
							limit 30'''.format(str(joined_string))

		cursorObj.execute(sql)

	__rows = cursorObj.fetchall()
	rows = sorted(__rows, key=lambda __rows: __rows[2])

	if rows: await msg.edit('Enviando {} resultados....'.format(len(rows)))
	
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

async def getSeries(con,message,title):

	logger.info("getSeries[{}]".format(title))
	cursorObj = con.cursor()
	msg = await message.edit('Buscando Series...')

	if title != '/serie' and title != '/series': 
		cursorObj.execute('''select series.id, series.name, series.sort 
                    			from series
								where series.sort LIKE '%{}%' 
								limit 30 '''.format(title))
	else:
		cursorObj.execute('''select series.id, series.name, series.sort from series
							 ORDER BY RANDOM()
							 limit 30 '''.format(title))


	__rows = cursorObj.fetchall()
	rows = sorted(__rows, key=lambda __rows: __rows[1])

	if rows: await msg.edit('Enviando {} resultados....'.format(len(rows)))
	
	temp = ''
	sending = 0
	for row in rows:
		sending +=1
		id, name, sort = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		temp += "[{}] \U0001F4DA {} /se{} \n".format(sending, sort,id)

	if rows:
		await msg.edit("Seleccione una Serie:\n" + temp)
	else: await msg.edit('No se encontraron resultados')

async def getSeriesbyAutor(con,message,title):

	logger.info("getSeriesbyAutor[{}]".format(title))
	cursorObj = con.cursor()
	msg = await message.edit('Buscando Series...')

	if title != '/serieautor': 
		cursorObj.execute('''select 
                    			books.id, 
                       			books.author_sort,
                          		series.name,
								books_series_link.series,
        						books_authors_link.author,
              					count(books.series_index) as count
							from books
								INNER JOIN books_authors_link
								ON books_authors_link.book = books.id
								INNER JOIN books_series_link
								ON books_series_link.book = books.id
								INNER JOIN series
								ON series.id = books_series_link.series
								where books.author_sort like "%{}%"
								GROUP BY books_series_link.series
								order by books.author_sort,books_series_link.series
								limit 30
						'''.format(title))
	else:
		cursorObj.execute('''select books.id, books.author_sort,series.name,
							books_series_link.series,books_authors_link.author,count(books.series_index) as count
							from books
							INNER JOIN books_authors_link
							ON books_authors_link.book = books.id
							INNER JOIN books_series_link
							ON books_series_link.book = books.id
							INNER JOIN series
							ON series.id = books_series_link.series
							--where books.author_sort like "%king%"
							GROUP BY books_series_link.series
							order by RANDOM() limit 50'''.format(title))


	__rows = cursorObj.fetchall()
	rows = sorted(__rows, key=lambda __rows: __rows[1])

	if rows: await msg.edit('Enviando {} resultados....'.format(len(rows)))
	
	temp = ''
	sending = 0
	for row in rows:
		sending +=1
		id, author_sort, name, series, author, count = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		#temp += f"[{sending}] \U0001F4DA {author_sort} - {name}({count}) /s{series}a{author} \n"
		temp += f"[{sending}] \U0001F4DA {author_sort} - {name} ({count}) /se{series} \n"

	if rows:
		await msg.edit("Seleccione una Serie:\n" + temp)
	else: await msg.edit('No se encontraron resultados')



async def getBooksbyAutor(con,message,BooksbyAutor):

	logger.info("getBooksbyAutor[{}]".format(BooksbyAutor))
	cursorObj = con.cursor()
	msg = await message.edit('Buscando...')

	if BooksbyAutor == '': 
		await message.edit('No se encontraron resultados')
		return 

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
							from books
							INNER JOIN data	
							ON books.id = data.book
							INNER JOIN books_authors_link
							ON books_authors_link.book = books.id
							where books_authors_link.author = {}
							order by books.author_sort,books.title 
							-- limit 150						
					'''.format(BooksbyAutor))

	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} Resultados....'.format(len(rows)))
	
	__temp = ''
	
	sending = 0
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		__temp += "[{}] \U0001F4DA {} /bm{} \n".format(sending, name,id)

	if rows:
		#await msg.edit("Seleccione un libro para descargar: /tdax{} (TODO)\n{}".format(BooksbyAutor,temp))

		tmp = "Seleccione un libro para descargar: /tdax{} (TODO)\n{}".format(BooksbyAutor,__temp)
		text, entities = markdown.parse(tmp)

		for text, entities in split_text(text, entities):
			await message.reply(text, formatting_entities=entities)

  
  
  
	else: await msg.edit('No se encontraron resultados')

async def getBooksbySeries(con,message,getBooksbySeries):
	logger.info("getBooksbySeries[{}]".format(getBooksbySeries))

	if getBooksbySeries == '': 
		await message.edit('No se encontraron resultados')
		return 

	cursorObj = con.cursor()
	msg = await message.edit('Buscando...')

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format,books.series_index
							from books
							INNER JOIN data	
							ON books.id = data.book
							INNER JOIN books_series_link
							ON books_series_link.book = books.id
							where books_series_link.series = {}
							order by books.author_sort,books.series_index,books.title
					'''.format(getBooksbySeries))

	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} Resultados....'.format(len(rows)))
	
	__temp = ''
	
	sending = 0

	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format,series_index = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		
		__temp += "[{sending}] \U0001F4DA ({series_index}) {name} /bm{id} \n".format(
												sending=sending, name=name,series_index=series_index,id=id)
	if rows:
		
		tmp = "Seleccione un Libro para descargar: /tdse{}\n{}".format(getBooksbySeries,__temp)
		text, entities = markdown.parse(tmp)

		for text, entities in split_text(text, entities):
			await message.reply(text, formatting_entities=entities)

	else: await msg.edit('No se encontraron resultados')

async def getBooksAll(con,message,title):

	logger.info("getBooksAll[{}]".format(title))
	cursorObj = con.cursor()
	msg = await message.edit('Buscando...')

	if title == '': 
		await msg.edit('No se encontraron resultados')
		return 

	cursorObj.execute('''select books.id, books.author_sort, books.title, books.path,data.name,data.format 
							from books
							INNER JOIN data	
							ON books.id = data.book
							where data.name LIKE '%{}%' 
							order by books.author_sort,books.title limit 30'''.format(title))


	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} resultados....'.format(len(rows)))
	
	temp = ''
	sending = 0
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format = row
		#logger.info("{}{}{}{}{}{}".format(id,author_sort,title,path,name,format))
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		temp += "[{sending}] \U0001F4DA {name} /bm{id} \n".format(
														sending=sending,name=name,id=id)

	if rows:
		await msg.edit("Seleccione un libro para descargar:\n" + temp)
	else: await msg.edit('No se encontraron resultados')

async def getAllBooksbyAutor(con,message,BooksbyAutor):

	logger.info("getBooksbyAutor[{}]".format(BooksbyAutor))
	real_id = get_peer_id(message.peer_id)
	CID , peer_type = resolve_id(real_id)

	cursorObj = con.cursor()
	msg = await message.edit('Buscando...')

	if BooksbyAutor == '': 
		await message.edit('No se encontraron resultados')
		return 

	cursorObj.execute('''	select books.id, books.author_sort, books.title, 
								books.path, data.name, data.format, books.has_cover, comments.text
							from books
							INNER JOIN data	
							ON books.id = data.book
							INNER JOIN books_authors_link
							ON books_authors_link.book = books.id
							INNER JOIN comments
							ON books.id = comments.book
							where books_authors_link.author = {} and books.id = comments.book
							order by books.author_sort,books.title 
							limit 50						
					'''.format(BooksbyAutor))

	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} Resultados....'.format(len(rows)))
	
	temp = ''
	
	sending = 0
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format,has_cover,text = row
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		if has_cover: cover = os.path.join(TG_BOOKS_PATH,path, '{}'.format('cover.jpg'))
		if os.path.exists(file):
			await msg.edit('Enviando [{}/{}] {}...'.format(sending,len(rows),title))
			loop = asyncio.get_event_loop()
			if os.path.exists(cover):
				resena = "{}".format(text[:max_text] + "...") if len(text) > max_text else text
				await client.send_file(CID, cover,caption=resena)
				await tg_send_file(CID,file,name)



		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		temp += "[{}] \U0001F4DA {} /bm{} \n".format(sending, name,id)

	if rows:
		await msg.edit("Seleccione un libro para descargar: /tdax{}\n{}".format(BooksbyAutor,temp))
	else: await msg.edit('No se encontraron resultados')

async def getAllBooksbySeries(con,message,getAllBooksbySeries):
	logger.info("getAllBooksbySeries[{}]".format(getAllBooksbySeries))
	real_id = get_peer_id(message.peer_id)
	CID , peer_type = resolve_id(real_id)

	if getAllBooksbySeries == '': 
		await message.edit('No se encontraron resultados')
		return 

	cursorObj = con.cursor()
	msg = await message.edit('Buscando...')

	cursorObj.execute('''select books.id, books.author_sort, books.title, 
							books.path,data.name,data.format,books.series_index,books.has_cover
						from books
						INNER JOIN data	
						ON books.id = data.book
						INNER JOIN books_series_link
						ON books_series_link.book = books.id
						where books_series_link.series = {}
						order by books.author_sort,books.series_index,books.title 		 						
					'''.format(getAllBooksbySeries))

	rows = cursorObj.fetchall()

	if rows: await msg.edit('Enviando {} Resultados....'.format(len(rows)))
	
	temp = ''
	
	sending = 0
	_buttons = []
	for row in rows:
		sending +=1
		id,author_sort,title,path,name,format,series_index,has_cover = row
		
		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		if has_cover: cover = os.path.join(TG_BOOKS_PATH,path, '{}'.format('cover.jpg'))
		if os.path.exists(file):
			await msg.edit('Enviando [{}/{}] {}...'.format(sending,len(rows),title))
			loop = asyncio.get_event_loop()
			if os.path.exists(cover):
				await client.send_file(CID, cover)
				await tg_send_file(CID,file,name)


		file = os.path.join(TG_BOOKS_PATH,path, '{}.{}'.format(name,format.lower()))
		temp += "[{sending}] \U0001F4DA ({series_index}) {name} /bm{id} \n".format(
												sending=sending, name=name,series_index=series_index,id=id)
	if rows:
		await msg.edit("Seleccione un Libro para descargar: /tdse{}\n{}".format(getAllBooksbySeries,temp))
	else: await msg.edit('No se encontraron resultados')



''' ------------------------------------ '''
''' ------------------------------------ '''
''' ------------------------------------ '''
async def worker(name):
	while True:
		# Esperando una unidad de trabajo.

		try:

			queue_item = await queue.get()
			#logger.info(f"INIT worker ['worker']")
			update = queue_item[0]
			message = queue_item[1]
			
			msg = update.message.message

			logger.info("worker ==> [{}]".format(update.message.message))

			real_id = get_peer_id(update.message.peer_id)
			CID , peer_type = resolve_id(real_id)

			if update.message.message not in command_tasks:
				command_tasks.append(update.message.message)
				logger.info("command_tasks ==> [{}]".format(command_tasks))


				if ((update.message.message).startswith('/title')):
					message = await update.reply('Search in queue...')
					logger.info("SEND BOOKS /title")
					rest = await getBooksTitle(con,message,msg.replace('/title ',''))
				
				elif ((update.message.message).startswith('/autor')):
					message = await update.reply('Search in queue...')
					logger.info("SEND BOOKS /autor:[%s]",msg)
					rest = await getAuthors(con,message,msg.replace('/autor ',''))
					
				elif ((update.message.message).startswith('/serieautor')):
					message = await update.reply('Search in queue...')
					logger.info("SEND serieautor :[%s]",msg)
					rest = await getSeriesbyAutor(con,message,msg.replace('/serieautor ',''))

				elif ((update.message.message).startswith('/serie')):
					message = await update.reply('Search in queue...')
					logger.info("SEND SERIES :[%s]",msg)
					rest = await getSeries(con,message,msg.replace('/serie ','').replace('/series ',''))


				
				
				elif ((update.message.message).startswith('/bm')):
					message = await update.reply('Search in queue...')
					m = re.search('/bm(.+?)(?=@).*', msg)
					if m:
						rest = await getBooksbyID(con,message,m.group(1))
						#await update.reply('Todos los archivos enviados')
					else:
						rest = await getBooksbyID(con,message,msg.replace('/bm',''))
						#await update.reply('Todos los archivos enviados')
		
				elif ((update.message.message).startswith('/ax')):
					message = await update.reply('Search in queue...')
					m = re.search('/ax(.+?)(?=@).*', msg)
					if m:
						rest = await getBooksbyAutor(con,message,m.group(1))
					else:
						rest = await getBooksbyAutor(con,message,msg.replace('/ax',''))

				elif ((update.message.message).startswith('/se')):
					message = await update.reply('Search in queue...')
					m = re.search('/se(.+?)(?=@).*', msg)
					if m:
						rest = await getBooksbySeries(con,message,m.group(1))
					else:
						rest = await getBooksbySeries(con,message,msg.replace('/se',''))


				elif ((update.message.message).startswith('/all')):
					message = await update.reply('Search in queue...')
					logger.info("SEND BOOKS :[%s]",msg)
					rest = await getBooksAll(con,message,msg.replace('/all ',''))
					logger.info("FINISH SEND BOOKS :[%s]",msg)

				elif ((update.message.message).startswith('/tdax')):
					message = await update.reply('Search in queue...')
					m = re.search('/tdax(.+?)(?=@).*', msg)
					if m:
						rest = await getAllBooksbyAutor(con,message,m.group(1))
						#await update.reply('Todos los archivos enviados')
					else:
						rest = await getAllBooksbyAutor(con,message,msg.replace('/tdax',''))
						#await update.reply('Todos los archivos enviados')

				elif ((update.message.message).startswith('/tdse')):
					message = await update.reply('Search in queue...')
					m = re.search('/tdse(.+?)(?=@).*', msg)
					if m:
						rest = await getAllBooksbySeries(con,message,m.group(1))
						#await update.reply('Todos los archivos enviados')
					else:
						rest = await getAllBooksbySeries(con,message,msg.replace('/tdse',''))
						#await update.reply('Todos los archivos enviados')

				#else:
					#await message.edit('Busqueda incorrecta, use /help para más ayuda')

				command_tasks.remove(update.message.message)
				#logger.info(command_tasks)
			else:
				logger.info('EXIST ELEMENTE: %s ', update.message.message)
				message = await update.reply('Ya existe una busqueda con estos parametros...')

			#logger.info(f"OUT worker ['worker']")

		except Exception as e:
			command_tasks.remove(update.message.message)
			logger.info('ERROR: %s Books Upload: %s' % (e.__class__.__name__, str(e)))
			message = await update.reply('ERROR: %s Books Upload: %s' % (e.__class__.__name__, str(e)))
			queue.task_done()
			continue
		


		# Unidad de trabajo terminada.
		queue.task_done()

client = TelegramClient(session, api_id, api_hash, proxy = None, request_retries = 10, flood_sleep_threshold = 120)

@events.register(events.NewMessage)
async def handler(update):
	global temp_completed_path
	global FOLDER_GROUP
	try:


		real_id = get_peer_id(update.message.peer_id)
		CID , peer_type = resolve_id(real_id)

		if not TG_AUTHORIZED_USER_ID or CID in usuarios:
			if update.message.message == '/help' or update.message.message == '/start':
				message = await update.reply(HELP) 
			elif update.message.message == '/version': 
				message = await update.reply(VERSION)
			elif update.message.message == '/alive': 
				message = await update.reply('Keep-Alive')
			elif update.message.message == '/me': 
				message = await update.reply('me: {}'.format(CID) )

			elif ((update.message.message).startswith('/')):
				#message = await update.reply('Search in queue...')
				await queue.put([update, update.message.message])
				#logger.info('Search in queue...')

		
		elif update.message.message == '/me': 
			logger.info('UNAUTHORIZED USER: %s ', CID)
			message = await update.reply('UNAUTHORIZED USER: %s \n add this ID to TG_AUTHORIZED_USER_ID' % CID)
	except Exception as e:
		message = await update.reply('ERROR: ' + str(e))
		logger.info('EXCEPTION USER: %s ', str(e))

try:
	# Crear cola de procesos concurrentes.
	tasks = []
	command_tasks = []
	for i in range(number_of_parallel_downloads):
		loop = asyncio.get_event_loop()
		task = loop.create_task(worker('worker-{%i}' %i))
		tasks.append(task)

	# Arrancamos bot con token
	client.start(bot_token=str(bot_token))
	client.add_event_handler(handler)

	# Pulsa Ctrl+C para detener
	loop.run_until_complete(tg_send_message("Calibre Upload Started: {version}".format(version=VERSION)))
	logger.info("%s" % VERSION)
	logger.info("********** Bot Books Upload Started **********")



	client.run_until_disconnected()
finally:
	# Cerrando trabajos.
	
	#f.close()
	for task in tasks:
		task.cancel()
	# Cola cerrada
	# Stop Telethon
	client.disconnect()
	logger.info("********** STOPPED **********")
	
