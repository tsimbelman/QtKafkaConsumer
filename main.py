# -*- coding: utf-8 -*-
from PyQt5 import QtCore, QtWidgets, uic
from kafka import KafkaConsumer
from random import randint
import json, sys, os, datetime, configparser
# ~ import subprocess,platform,  time


class DownloadThread(QtCore.QThread):
	data_downloaded = QtCore.pyqtSignal(object,object)

	def __init__(self,notify,group,offset,auto_commit,servers):
		QtCore.QThread.__init__(self)
		self.consumer = KafkaConsumer(notify,
					 group_id = group ,
					 bootstrap_servers= servers,
					 auto_offset_reset= offset,
					 enable_auto_commit = auto_commit,
					 )
		
	def run(self):
		try:
			for message in self.consumer:
				try:
					payload = message.value.decode('utf-8')				
					payload = payload.rstrip(' \t\r\n\0')
				except:
					try:
						payload = message.value.decode('utf-8', errors='replace').strip().encode(sys.stdout.encoding, errors='replace')
						payload = payload.rstrip(' \t\r\n\0')
					except:
						pass				
				self.data_downloaded.emit(message,payload)				
		except:
			pass		


		
class MainWindow(QtWidgets.QDialog):

	def __init__(self):
		super(MainWindow, self).__init__()
		uic.loadUi('appData/gui/main.ui', self)
		self.btnStart.clicked.connect(self.startCollect)
		self.btnStop.clicked.connect(self.stopCollect)
		self.btnClean.clicked.connect(self.cleanOutput)
		self.btnFile.clicked.connect(self.openFile)
		self.btnHideFile.clicked.connect(self.hideFile)			
		
		try:
			self.appConfig = configparser.ConfigParser()		
			self.appConfig.read("appData/config/appConfig.ini") 
			
			self.appVersion.setText("Version: "+self.appConfig['appConf']['AppVersion'])
			self.checkBoxCase.setChecked(self.appConfig.getboolean('appConf','CaseSensitive'))
			self.checkBoxTree.setChecked(self.appConfig.getboolean('appConf','ExpandJson'))
			self.checkBoxFile.setChecked(self.appConfig.getboolean('appConf','WritetoFile'))
			
			self.checkBoxLocation.setChecked(self.appConfig.getboolean('settings','AddLocation'))
			self.checkBoxcCommit.setChecked(self.appConfig.getboolean('settings','AutoCommit'))

			topicNames = self.appConfig['settings']['TopicNames']
			topicNames = topicNames.split(',')	
			for topic in topicNames:
				self.comboTopicName.addItem(topic)
					
			envList = self.appConfig['settings']['Environment']
			envList = envList.split(',')	
			for env in envList:
				self.comboBoxEnv.addItem(env)	

			locations = self.appConfig['settings']['Locations']
			locations = locations.split(',')	
			for location in locations:
				self.comboLocation.addItem(location)				
				
			offsets = self.appConfig['settings']['Offsets']
			offsets = offsets.split(',')	
			for offset in offsets:
				self.comboOffset.addItem(offset)	
			
			self.lineGroup.setText("group_"+str(randint(10000, 99999))+"_"+str(randint(10000, 99999)) )	
		except:
			self.appVersion.setText('File "appConfig.ini" is corrupt or not found') 
		
		self.btnFile.setVisible(False)
		self.btnHideFile.setVisible(False)
		
		self.comboTopicName.currentIndexChanged.connect(self.updateConfiguration)
		self.comboLocation.currentIndexChanged.connect(self.updateConfiguration)
		self.checkBoxLocation.stateChanged.connect(self.updateConfiguration)
		self.comboBoxEnv.currentIndexChanged.connect(self.updateConfiguration)
		
		self.styleText = '<span style="background-color: #FFFF00">%s</span>'
		self.defaultText = '<p>%s</p>'
		self.updateConfiguration()
		self.show()


		
	def updateConfiguration(self):
		try:
			self.plainServers.clear()
			env = self.comboBoxEnv.currentText()
	
			if self.checkBoxLocation.isChecked():
				location = self.comboLocation.currentText()		
			else:
				location = ""
								
			serverList = ""
			if  self.comboLocation.currentIndex() == 0:
				serverList =  self.appConfig[env][  self.appConfig['settings']['variable1'] ]
			elif self.comboLocation.currentIndex() == 1:		
				serverList =  self.appConfig[env][  self.appConfig['settings']['variable2'] ]
			serverList = serverList.split(',')	
			for server in serverList:
				self.plainServers.appendPlainText(server)
					
							
			if self.comboTopicName.currentIndex() == 0:
				self.lineTopicName.setText( self.appConfig[env][  self.appConfig['settings']['variable3'] ]+location)
			elif self.comboTopicName.currentIndex() == 1:
				self.lineTopicName.setText( self.appConfig[env][  self.appConfig['settings']['variable4'] ]+location)
			elif self.comboTopicName.currentIndex() == 2:
				self.lineTopicName.setText( self.appConfig[env][  self.appConfig['settings']['variable5'] ]+location)
		except:
			pass
			
	def hideFile(self):
		self.btnFile.setVisible(False)
		self.btnHideFile.setVisible(False)
		self.checkBoxFile.setChecked(False)
		self.labelFile.setText("")	
			
	def openFile(self):
		try:
			if os.name == 'posix':
				os.system("xdg-open " + "Results/result_"+self.startTime+".json")
			else:
				os.system("start " + "Results/result_"+self.startTime+".json")
		except:
			pass


	def cleanOutput(self):
		self.editNotifications.clear()


	def stopCollect(self):
		self.downloader.consumer.close(autocommit=False)
		self.lineGroup.setText("group_"+str(randint(10000, 99999))+"_"+str(randint(10000, 99999)) )	
		self.btnStart.setEnabled(True)
		self.btnStop.setEnabled(False)

		self.editNotifications.append(self.defaultText % "----------------")		
		self.editNotifications.append( self.defaultText % ( datetime.datetime.today().strftime("%m-%d-%Y-%T").replace(":","-",4) + " ---- Job is stopped"   ))
		
		if self.checkBoxFile.isChecked():
			with open(self.fileName, "a") as f:
				f.write(os.linesep)
				f.write(self.startTime + " ---- Job is stopped" + os.linesep)


	def startCollect(self):
		self.btnStart.setEnabled(False)
		self.btnStop.setEnabled(True)
		
		
		
		self.startTime = datetime.datetime.today().strftime("%m-%d-%Y-%T").replace(":","-",4)

		self.filterText = str(self.lineFilter.text())
		group = str(self.lineGroup.text())
		offset = str(self.comboOffset.currentText())
		auto_commit = self.checkBoxcCommit.isChecked()
		notify = self.lineTopicName.text()
		servers = []
		servers.extend(str(self.plainServers.toPlainText()).split('\n'))

		self.editNotifications.append(self.defaultText % (self.startTime + " ---- Starting job..."))


		if self.checkBoxFile.isChecked():
			self.fileName = str('Results/result_'+self.startTime+'.json')
			self.labelFile.setText("result_"+self.startTime+".json")
			self.btnFile.setVisible(True)
			self.btnHideFile.setVisible(True)
			with open(self.fileName, "a") as f:
				f.write(self.startTime + " ---- Starting job..." + os.linesep + os.linesep)

		self.downloader = DownloadThread(notify,group,offset,auto_commit,servers)
		self.downloader.data_downloaded.connect(self.on_data_ready)
		self.downloader.start()

		
	def on_data_ready(self, message,payload):
		message =   "%s : %d : %d key = %s" % (message.topic, message.partition, message.offset, message.key)
		value = "value=%s" % (payload)
		value = value.replace("\n", ' ', 50)
		value = value.replace("{", '\n{', 1)
		self.filterText = str(self.lineFilter.text())
		
		if self.checkBoxTree.isChecked():
			value = value.replace("{", '{\n', 50)
			value = value.replace(",", ',\n', 50)

		if  self.filterText == "":
			self.editNotifications.append(self.defaultText % "----------------")			
			self.editNotifications.append(self.defaultText % message)
			self.editNotifications.append(self.defaultText % value)

			if self.checkBoxFile.isChecked():
				with open(self.fileName, "a") as f:
					f.write("----------------"+ os.linesep)
					f.write(message + os.linesep)
					f.write(value + os.linesep + os.linesep)	
		else:
			upperValue = value		
			upperMessage = message		
			if not self.checkBoxCase.isChecked():
				value = value.lower()
				message = message.lower()
				self.filterText = self.filterText.lower()			
			if (self.filterText in value) or (self.filterText in message):
				self.editNotifications.append(self.defaultText % "----------------")
				value = value.replace(self.filterText, self.styleText % self.filterText, 20)
				message = message.replace(self.filterText, self.styleText % self.filterText, 20)
				self.editNotifications.append(self.defaultText % message)
				self.editNotifications.append(self.defaultText % value)
				if self.checkBoxFile.isChecked():
					with open(self.fileName, "a") as f:
						f.write("----------------"+ os.linesep)
						f.write(upperMessage + os.linesep)
						f.write(upperValue + os.linesep + os.linesep)	


if __name__ == '__main__': 
	app = QtWidgets.QApplication(sys.argv)
	main_window = MainWindow()
	sys.exit(app.exec_())

