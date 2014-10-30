#!/usr/bin/env python
# -*- coding: cp1251 -*-

##################################################################
#                          Процедуры                             #
##################################################################
# процедура синхранизации каталогов.
# входные параметры
# from_disk - с диска
# to_disk   - на диск
# source    - каталог источник
# на целевом диске создается одноименный каталог для данных с диска источника
def sync(source, target):
	from os import path, getcwd, mkdir, system
	from subprocess import Popen, PIPE
	# если в каталоге скрипта отсутствует каталог LOG создаем его
	if not(path.exists(getcwd() + '\\LOG')) or not(path.isdir(getcwd() + '\\LOG')):
			mkdir(getcwd() + '\\LOG')
	log_file = source.split('\\')[1]+'.txt'
	# Выплнить комманду OS robocopy
	# from_disk + ':\\' + source - копировать каталог источник
	# to_disk + ':\\' + source   - целевой каталог
	#
	# /sec     - копировать файлы с параметрами безопастности
	# /E       - копировать вложенные папки включая пустые
	# /PURGE   - удалять файлы в папке назначения которых более не существует в источнике
	# /XO      - исключить более ранние файлы
	# /R:3     - число повторных попыток при неудаче копирование
	# /W:5     - интервал между попытками
	# /UNILOG:    - имя лог файла (формат лога в юникоде)
	# /NP      - не отображать скопированное в %
	proc = Popen('robocopy.exe ' + source +' '+ target + ' /SECFIX /COPYALL /E /PURGE /XO /R:3 /W:5 /LOG:'+ getcwd() + '\\LOG\\'+ log_file + ' /NP', shell=True, stdout=PIPE)
	proc.wait()
	out = proc.stdout.readlines()
	
	if path.exists(getcwd() + '\\LOG\\'+ log_file) and path.isfile(getcwd() + '\\LOG\\'+ log_file):
		# открыть полученный файл лога в кодировке cp866 и сконвертировать в cp1251
		text = open(getcwd() + '\\LOG\\'+ log_file, 'r').read().decode('cp866').encode('cp1251')
		# сохранить в новой кодировке
		open(getcwd() + '\\LOG\\'+ log_file, 'w').write(text)
		
		return getcwd() + '\\LOG\\'+ log_file
	else:
		return False

# процедура отправки почты 
def send_mail(send_from, send_to, subject, text, files=[], server='smtp.cons.tsk.ru'):
	from smtplib import SMTP
	from os import path
	from email.MIMEMultipart import MIMEMultipart
	from email.MIMEBase import MIMEBase
	from email.MIMEText import MIMEText
	from email.Utils import COMMASPACE, formatdate
	from email import Encoders
	from email.header import Header

	assert type(send_to)==list
	assert type(files)==list
	msg = MIMEMultipart()
	msg['From'] = Header(send_from.decode("utf-8")).encode()
	msg['To'] = Header(COMMASPACE.join(send_to).decode("utf-8")).encode()
	msg['Date'] = formatdate(localtime=True)
	msg['Subject'] = Header(subject.decode("utf-8")).encode()
	msg.attach( MIMEText(text,'plain','UTF-8') )

	for f in files:
		part = MIMEBase('application', "octet-stream")
		part.set_payload( open(f,"rb").read() )
		Encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="%s"' % path.basename(f))
		msg.attach(part)
	smtp = SMTP(server, 25)
	smtp.sendmail(send_from, send_to, msg.as_string())
	smtp.close()		

# Функция расчета контрольной суммы файла  
def getMD5sum(fileName):
	from hashlib import md5
	m = md5()
	fd = open(fileName, 'rb')
	b = fd.read()
	m.update(b)
	fd.close()
	return m.hexdigest()

def comp (source_patch, target_patch):
	from os import path, walk
	from filecmp import cmp
	# выходное сообщение о найденных отличиях
	message = ''
	path_f = []
	tree = walk(source_patch)
	for d, dirs, files in tree:
		for f in files:
			patch = path.join(d,f) # формирование адреса
			path_f.append(patch)      # добавление адреса в список
		# перибираем адреса файлов из списка
		for patch in path_f:
			# выполняем сравнение файлов и в случае отличий получаем информацию о файлах
			# проверяем существование файла
			if not path.exists(patch.replace(source_patch, target_patch)):
				message = message + 'Отсутствует целевой файл: '+ patch.replace(source_patch, target_patch)
			# сверяем размеры исходного и целевого файла
			elif path.getsize(patch.replace(source_patch, target_patch)) <> path.getsize(patch):
				message = message + file_info(patch, patch.replace(source_patch, target_patch))
			# дата последней модификации
			elif path.getmtime(patch.replace(source_patch, target_patch)) <> path.getmtime(patch):
				message = message + file_info(patch, patch.replace(source_patch, target_patch))
			# сравниваем файлы
			elif not cmp(patch.replace(source_patch, target_patch), patch):
				message = message + file_info(patch, patch.replace(source_patch, target_patch))
		return message

# функция вывода информации о файлах.
# по идее правильнее сразу на вход подавать список файлов
def file_info(in_file, out_file):
	from os import path
	from datetime import datetime
	message = ''
	# в данном случае формируем список в программе из двух входных элементов
	file_list = []
	file_list.append(in_file)
	file_list.append(out_file)
	# цикл перебирает список
	for f in file_list:
		message = message + f + '\n'
		message = message + '    Размер файла:      '+ str(path.getsize(f)/1024) + ' Kb\n'
		message = message + '    Дата модификации:  ' + datetime.fromtimestamp(path.getmtime(f)).strftime("%d-%m-%Y %H:%M:%S") + '\n'
		message = message + '    Дата обращения:    ' + datetime.fromtimestamp(path.getatime(f)).strftime("%d-%m-%Y %H:%M:%S") + '\n'
		message = message + '    Контрольная сумма: ' + getMD5sum(f) + '\n'
		message = message + '--------------------------------------------------------------\n'
	return message

##################################################################
#                    Основная программа                          #
##################################################################
if __name__ == "__main__":
	####################
	##     модули     ##
	####################
	from os import path, getcwd, chdir, remove
	from sys import exit
	from subprocess import Popen, PIPE
	from time import sleep
	from types import NoneType
	####################
	##   Переменные   ##
	####################
	work_dir = 'C:\Windows\!Script\Backup'
	source_disk = 'c:'
	destin_disk = 'o:'
	param_script = 'vs_generated.cmd'
	# список каталогов для копирования
	cwd_list = [['o:\\Windows\\!Script\\Backup', '\\\\master\\apps\\temp\\viv\\backup'],['o:\\ocs-ng', '\\\\master\\apps\\temp\\viv\\ocs-ng']]
	# настройка почты
	from_addr = 'viv@cons.tsk.ru'
	tech_addr = ['viv@cons.tsk.ru', 'taa@cons.tsk.ru']
	# прочие переменные 
	files_attach = [] # список файлов для вложения
	msg = ''          # тело письма
	##################
	##    main      ##
	##################
	chdir(work_dir)
	# проверяем наличие файла создания теневых копий
	# без него работать не будем поэтому выход с ошибкой
	if not(path.exists(getcwd() + '\\vshadow.exe')) and not(path.isfile(getcwd() + '\\vshadow.exe')) :
		exit(1)
	proc = Popen('vshadow.exe -nw -p -script=' + param_script + ' ' + source_disk, shell=True, stdout=PIPE)
	proc.wait()
	out = proc.stdout.readlines()


	
	if path.exists(getcwd() + '\\' + param_script) and path.isfile(getcwd() + '\\' + param_script):
		# открыть и получить список строк файла
		prm_file = open (getcwd() + '\\' + param_script, 'r')
		prm_lines = prm_file.readlines()
		prm_file.close()
		SHADOW_ID = '' # переменная для идентификатора теневой копии
		# перебираем строки файла с выводом параметров создания копии
		for param_str in prm_lines:
			# если строка содержит идентификатор созданной копии
			if 'SET SHADOW_ID_1' in param_str:
				# заносим его в переменнцю
				SHADOW_ID = param_str.split('=')[1][:-1]
				break
		# если идентификатора в файле нет завершаем работу
		if SHADOW_ID == '':
			sys.exit(1)
		proc = Popen('vshadow.exe -el=' + SHADOW_ID + ',' + destin_disk, shell=True, stdout=PIPE)
		proc.wait()
		out = proc.stdout.readlines()
		# перебираем список каталогов для копирования
		for cwd_patch in cwd_list:
			# даем команду на синхронизацию каталогов
			log_name = sync(cwd_patch[0], cwd_patch[1])
			# обрабатываем логи для отправки письма
			if log_name:
					# открыть и получить список строк файла
					log_file = open (log_name, 'r')
					log_lines = log_file.readlines()
					# временный счетчик строк разделителей
					count = 0
					# перебираем последовательно строки
					for lines in log_lines:
							# если найден разделитель увеличиваем значение счетчика
							if lines.find('------------------------------------------')<> -1:
									count = count + 1
							# если счетчик не равен 3 и строка не пустая, добавляем ее к телу письма
							if count <> 3 and lines[:-1].strip <> '':
									msg = msg + lines.decode('cp1251').encode('utf8')
					# добавляем файл лога в список файлов вложений
					files_attach.append( log_name )
		# ###########################
		# сверяем синхронизацию
		# ###########################
		msg = msg + '\n\n'
		for cwd_patch in cwd_list:
			mail_head = '#'*(80+len(cwd_patch[0]))+'\n'
			mail_head = mail_head + '#'+' '*39 + cwd_patch[0] + ' '*39 + '#\n'
			mail_head = mail_head + '#'*(80+len(cwd_patch[0]))+'\n'
			# сравниваем каталоги
			message = comp(cwd_patch[0], cwd_patch[1])
			# если имеются отличия
			if message <> '' and type(message) <> NoneType:
				# добавляем полученное к сообщению для отправки
				msg = msg + mail_head + message.decode('cp1251').encode('utf8')
		# отправляем письмо техникам
		send_mail(from_addr, tech_addr, 'Sauna Sync Log & Verification File', msg, files_attach)
		
		proc = Popen('vshadow.exe -ds=' + SHADOW_ID, shell=True, stdout=PIPE)
		proc.wait()
		out = proc.stdout.readlines()
		remove(getcwd() + '\\' + param_script)
		exit(0)
	exit(1)
