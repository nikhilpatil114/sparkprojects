## FIND THE GIVEN STRINGS ARE ANAGRAMS OR NOT
str1 = 'listen'
str2 = 'silent'
s1 = sorted(str1)
s2 = sorted(str2)
if len(str1) != len(str2):
    print('given strings are not anagrams')
elif s1 == s2:
    print('given strings are anagrams')
else:
    print('given strings ar not anagrams')

##INPUT='ah2bj24k'  OUTPUT=224ahbjk
str1 = 'ah2bj24k'
strs = []
strn = []
for i in str1:
    if i.isalpha():
        strs.append(i)
    else:
        strn.append(i)
RES = strn + strs
print("".join(RES))

##INPUT='a5b3d2'  OUTPUT=aaaaabbbdd
str = 'a5b3d2'
output = ""
for i in str:
    if i.isalpha():
        res = i
    else:
        n = int(i)
        output = output + (res * n)
print(output)


# INPUT='aadddccjjj'  OUTPUT='a2d3c2j3'

def compresion(a):
    n = len(a)
    new_str = ''
    count = 1

    for i in range(0, n - 1):
        if a[i] == a[i + 1]:
            count += 1
        else:
            new_str = new_str + a[i] + str(count)
            count = 1
    new_str = new_str + a[n - 1] + str(count)
    return new_str


print(compresion('aadddccjjj'))

##INPUT="Aditya"   OUTPUT=AdItYa
str = "Aditya"
lst = []
for i in str:
    if str.index(i) % 2 == 0:
        i1 = i.upper()
    else:
        i1 = i.lower()
    lst.append(i1)
print("".join(lst))

##EXTRACT 1ST CHARACTERS FROM STRINGS
inp = ["Aditya", "Praful", "Rahul", "Mayur", "Sachin"]
lst = []
for i in inp:
    lst.append(i[0])
print(lst)
print("".join(lst))


##REVERSE THE STRING USING RECURSION
def reverse(string):
    if len(string) == 1:
        return string
    else:
        return reverse(string[1:]) + string[0]


st = reverse("hello")
print(st)


##CHECK THE GIVEN NUMBER IS PALINDROM OR NOT
def palindrom(str):
    if str == str[::-1]:
        print("given string or number is palindom")
    else:
        print("given string or number are not palindrom")


palindrom('madam')
palindrom('9009')
palindrom('nikhil')

##EXTRACT THE DUPLICATE RECORD AND NON DUPLICATE RECORD
lst = [3, 4, 5, 6, 4, 7, 8, 9, 3, 44, 65, 35, 44]
lst1 = []
lst2 = []
for i in range(len(lst)):
    for j in range(i + 1, len(lst)):
        if lst[i] == lst[j] and lst[i] not in lst1:
            lst1.append(lst[i])
        elif lst[i] != lst[j] and lst[i] not in lst2:
            lst2.append(lst[i])

print(lst1)
print(lst2)

# NON DUPLICATE RECORD
lst = []
for i in lst:
    if i not in lst1:
        lst1.append(i)

print(lst1)

# DUPLICATE RECORD
d = []
for ele in lst:
    if lst.count(ele) > 1 and ele not in d:
        d.append(ele)
print(d)

## DELETE DUPLICATE RECORDS
lst = [3, 4, 5, 6, 4, 7, 8, 9, 3, 44, 65, 35, 44]
lst1 = set(lst)
print(lst1)

# OR
my_list = list(dict.fromkeys(lst))
print(my_list)

# SORT THE LIST WITHOUT USING SORT KEYWORD
lst = [44, 2, 5, 11, 4, 66, 3, 7, 1]
n = len(lst)
for i in range(n):
    for j in range(i + 1, n):
        if lst[i] > lst[j]:
            lst[i], lst[j] = lst[j], lst[i]
print(lst)

dict1 = {575: 'jalgaon', 876: 'mumbai', 132: 'pune', 745: 'nasik'}
d = sorted(dict1.keys())
dict2 = {}
for i in d:
    dict2[i] = dict1[i]
print(dict2)


# FOBINACCI SERIESE USING RECURSION
def recf(n):
    if n <= 1:
        return n
    else:
        return (recf(n - 1) + recf(n - 2))


d = int(input("enter a range"))
for i in range(d):
    print(recf(i))

##REVERSE THE STRING
str1 = "the sky in blue"
l = str.split(str1)
print(l)
n = l[::-1]
print(n)
rev = (" ".join(n))
print(rev)

str1 = "the sky in blue"
str2 = str1[::-1]
print(str2)

str1 = "the sky in blue"
l = str.split(str1)
print(l)
print(l[1], l[0], l[3], l[2])

# find the maximum and minimum number from list
lst = [44, 2, 5, 11, 4, 66, 3, 7, 1]
maximum = lst[0]
minimum = lst[0]
for i in lst:
    if i > maximum:
        maximum = i
    if i < minimum:
        minimum = i
print(maximum)
print(minimum)

##Exception handling  if element 1 in list
l = [1, 2, 3, 4]
sum = 0
for i in l:
    if i == 1:
        raise Exception('Exception:1 is found')
    else:
        sum += 1


def factorial(a):
    if a == 1:
        return 1
    else:
        return (a * factorial(a - 1))


print(factorial(5))

##LIST OF PRIME NUMBER
low = 1
upp = 100
for num in range(low, upp + 1):
    if num > 1:
        for i in range(2, num):
            if ((num % i) == 0):
                break
        else:
            print(num)

# GIVEN NUMBER IS PRIME OR NOT
N = 31
for i in range(2, round(N / 2)):
    if N % i == 0:
        print("number is not a prime")
        break

else:
    print("number is prime")

# DICTIONARY FROM TWO LIST
lst = [1, 2, 3, 4, 5]
lst1 = ['jalgaon', 'mumbai', 'pune', 'nasik', 'dhule']
new_dict = {lst[i]: lst1[i] for i in range(len(lst))}
print(new_dict)

# FIND THE MAXIMUM  NUMBER FROM ARRAY
a = [10, 49, 66, 98, 30, 20, 77, 40, 99]
for i in range(len(a) - 1):
    if a[i] >= a[i + 1]:
        a[i + 1] = a[i]
    elif a[i] <= a[i + 1]:
        pass
print(a[i])


def largest(arr, n):
    # Initialize maximum element
    max = arr[0]

    # Traverse array elements from second
    # and compare every element with
    # current max
    for i in range(1, n):
        if arr[i] > max:
            max = arr[i]
    return max


# Driver Code
arr = [10, 324, 45, 90, 9808]
n = len(arr)
Ans = largest(arr, n)
print("Largest in given array ", Ans)

lst = [1, 2, 3, 4, 5]
lst1 = ['jalgaon', 'mumbai', 'pune', 'nasik', 'dhule']
new_d = dict(zip(lst, lst1))
print(new_d)

lst = [1, 2, 3, 4, 5]
lst1 = ['jalgaon', 'mumbai', 'pune', 'nasik', 'dhule']
new_l = list(zip(lst, lst1))
print(new_l)

lst = [10, 20, 30, 40, 50, 60, 11, 32, 44, 33, 12]
x = list(map(lambda x: x * 2, lst))
print(x)

y = list(filter(lambda x: x % 2 == 0, lst))
print(y)

from functools import *

r = reduce(lambda x, y: x * y, lst)
print(r)

y = filter(lambda x: x % 2 == 0, lst)
for i in y:
    print(i)

x = map(lambda x: x * 2, lst)
for i in x:
    print(i)


class myclass:
    def add(self):
        print('yo are  in myclass')


class subclass(myclass):
    def add(self):
        print('you are in subclass')


c = subclass()
c.add()
d = myclass()
d.add()

str = 'madam'
str2 = ''
for i in range(len(str) - 1, -1, -1):
    str2 = str2 + str[i]
print(str2)
if str == str2:
    print('it is a palindrom')
else:
    print('it is not a palindrom')


def fact(n):
    if n == 1:
        return n
    else:
        return n * fact(n - 1)


y = fact(4)
print(y)


def recf(n):
    if n <= 1:
        return n
    else:
        return (recf(n - 1) + recf(n - 2))


d = int(input("enter a numer"))
for i in range(d):
    print(recf(i))

a = [10, 20, 30, 40, 70, 22, 11, 32, 21, 87]
max = a[0]
for i in range(1, len(a)):
    if a[i] > max:
        max = a[i]
print(max)


def compresion(a):
    n = len(a)
    new_str = ''
    count = 1

    for i in range(0, n - 1):
        if a[i] == a[i + 1]:
            count += 1
        else:
            new_str = new_str + a[i] + str(count)
            count = 1
    new_str = new_str + a[n - 1] + str(count)
    return new_str


print(compresion('aadddccjjj'))

ls = [1, 2, 3, 4, 5]
ls.reverse()

ls
#DICTIONARY FROM LIST AND COUNT THE STRINGS
try:
    i=["a","b","c","c","e","d","d"]
    d = {x: i.count(x) for x in i}
    print(d)
except Exception as e:
    print(e)
from collections import Counter
var=Counter(i)
lst=dict(var)

###############
lst=["matt","jayesh","garry","ramesh","lilly"]
for i in lst:
    for j in i.split():
        for k in range(len(j)-1):
            if j[k]==j[k+1]:
                print(j)

##################
lst=["matt","jayesh","garry","ramesh","lilly"]
for i in lst:
    for k in range(len(i)-1):
           if i[k]==i[k+1]:
               print(i)

