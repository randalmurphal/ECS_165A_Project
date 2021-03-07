from template.config import *
import threading

def int_to_bytes(val, num_bytes):
	return [(val & (0xff << pos*8)) >> pos*8 for pos in reversed(range(num_bytes))]

class Page:

	def __init__(self):
		self.num_records = 0
		self.data        = bytearray(4096)
		self.path        = ""
		self.locked_recs = {}

	def full(self):
		if self.num_records >= 512:
		    return True
		# Check if Full, so if bytes go over 4096 or 4 KB
		return False

	'''
		Write when inserting page
	'''
	def write(self, value):
		trans_num = threading.get_ident()
		if trans_num not in self.locked_recs.keys():
			self.locked_recs[trans_num] = []
		if (self.num_records+1) not in self.locked_recs[trans_num]:
			self.locked_recs[trans_num].append(self.num_records+1)
		else:
			return False
		offset = self.num_records * 8
		for i in int_to_bytes(value, 8):
			try:
				self.data[offset] = i
				offset            = offset + 1
			except IndexError:
				raise("IndexError:", offset, self.num_records)
		self.locked_recs[trans_num].remove(self.num_records+1)
		self.num_records += 1
		return True

	'''
		Write when creating an updated tail page
			- needs base path to see if record is being written to at this moment
	'''
	def write_tail(self, base_rec_num, value):
		trans_num = threading.get_ident()
		if base_rec_num in self.locked_recs[trans_num]:
			return False
		else:
			return self.write(value)

	def overWrite(self, value, record_num):
		offset = record_num * 8
		for i in int_to_bytes(value, 8):
			self.data[offset] = i
			offset = offset + 1
		return True

	def setPath(self,path):
		self.path = path

	def retrieve(self, record_num):
		trans_num = threading.get_ident()
		if record_num not in self.locked_recs[trans_num]:
			self.locked_recs[trans_num].append(record_num)
		else:
			return -1
		offset = record_num * 8
		temp = [0,0,0,0,0,0,0,0]
		for i in range(0,8):
			# try:
			temp[i] = self.data[offset]
			offset  = offset+1
			# except IndexError:
				# print("IndexError:", offset, self.num_records)
		return int.from_bytes(temp, byteorder='big')
