f = open("E:\\bigdatafile\\donation.csv", "r")
print(f.read())

#f = open("E:\\bigdatafile\\donation.csv", mode='r', encoding='utf-8')
print(f.read())
f.close()

with open("E:\\bigdatafile\\pyfile.txt",'w',encoding = 'utf-8') as f:
    f.write("my first file\n")
    f.write("This file\n\n")
    f.write("contains three lines\n")

#f = open("E:\\bigdatafile\\pyfile.txt", mode='r', encoding='utf-8')
print(f.read())

f = open("E:\\bigdatafile\\pyfile.txt", mode='r', encoding='utf-8')
for line in f:
     print(line, end = '')
