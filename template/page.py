# from config import *

def int_to_bytes(val, num_bytes):
	return [(val & (0xff << pos*8)) >> pos*8 for pos in reversed(range(num_bytes))]

class Page:

	def __init__(self):
		self.num_records = 0
		self.data = bytearray(4096)

	def full(self):
		if self.num_records >= 512:
		    return True
		# Check if Full, so if bytes go over 4096 or 4 KB
		return False

	def write(self, value):
		offset = self.num_records * 8
		for i in int_to_bytes(value, 8):
			self.data[offset] = i
			offset = offset + 1
		self.num_records += 1
		return True

	def retrieve(self, record_num):
		offset = record_num * 8
		temp = [0,0,0,0,0,0,0,0]
		for i in range(0,8):
			temp[i] = self.data[offset]
			offset  = offset+1
		return int.from_bytes(temp,byteorder='big')
