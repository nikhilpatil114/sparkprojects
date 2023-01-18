#string format
a=100
b=400
print('math:{} and phy:{}'.format(a,b))

print('{2} {1} {0}'.format('directions','the', 'Read'))

print('a: {a}, b: {b}, c: {c}'.format(a = 1, b = 'Two',c = 12.3))

name = 'Ele'
print(f"My name is {name}.")

print(f"He said his age is {(lambda x: x*2)(3)}")

print("The mangy, scrawny stray dog %s gobbled down"%'hurriedly'+' how are you')

print('Joe stood up and %s to the crowd.' % 'spoke')
print('There are %d dogs.' % 4)

print('The valueof pi is: %1.5f' % 3.141592)
# vs.
print('The valueof pi is: {0:1.5f}'.format(3.141592))



x = 'looked'
print("Misha %s and %s around" % ('walked', x))

print('The value of pi is: %5.4f' %(3.141592))


