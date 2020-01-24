#import file as f
f = open('airlines.csv','r')
#print(f.read())
f2 = open('abc.txt','w+')
#f2.write("Deepak")
#f2.write("Ram Jane")

for f1 in f.readlines():
    f2.write(f1)

f.close()
f2.close()
#f.deallocate()
