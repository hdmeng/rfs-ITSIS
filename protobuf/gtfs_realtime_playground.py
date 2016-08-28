# #! /usr/bin/python

from google.transit import gtfs_realtime_pb2
import sys
import urllib

feed = gtfs_realtime_pb2.FeedMessage()
# response = urllib.urlopen('http://70.232.147.132/rtt/public/utility/gtfsrealtime.aspx/alert')
# response = urllib.urlopen('http://70.232.147.132/rtt/public/utility/gtfsrealtime.aspx/tripupdate')
response = urllib.urlopen('http://api.bart.gov/gtfsrt/tripupdate.aspx')
# response = urllib.urlopen('http://api.bart.gov/gtfsrt/alerts.aspx')
feed.ParseFromString(response.read())
for entity in feed.entity:
  if entity.HasField('trip_update'):
    print entity.trip_update
  if entity.HasField('alert'):
    print entity.alert

# # Iterates through all people in the AddressBook and prints info about them.
# def ListPeople(address_book):
# 	for person in address_book.person:
# 		print "Person ID:", person.id
# 		print "  Name:", person.name
# 		if person.HasField('email'):
# 			print "  E-mail address:", person.email

# 		for phone_number in person.phone:
# 			if phone_number.type == addressbook_pb2.Person.MOBILE:
# 				print "  Mobile Phone #: ",
# 			elif phone_number.type == addressbook_pb2.Person.HOME:
# 				print "  Home Phone #: ",
# 			elif phone_number.type == addressbook_pb2.Person.WORK:
# 				print "  Work Phone #: ",
# 			print phone_number.number

# # Main Procedure: Reads the entire address book from a file and prints all the 
# # 	information inside
# if len(sys.argv) != 2:
# 	print "Usage:", sys.argv[0], "ADDRESS_BOOK_FILE"
# 	sys.exit(-1)

# address_book = gtfs_realtime_pb2.AddressBook()

# # Read the existing address book.
# f = open(sys.argv[1], "rb")
# address_book.ParseFromString(f.read())
# f.close()

# ListPeople(address_book)
