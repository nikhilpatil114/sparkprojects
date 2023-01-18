import re
#substitute

from re import *
data='how are you nikhil where you from i am from jalgaon'
data1=re.sub('nikhil','mayur',data)
print(data1)

#split
# '\W+' denotes Non-Alphanumeric Characters
# or group of characters Upon finding ','
# or whitespace ' ', the split(), splits the
# string from that point
print(split('\W+', 'Words, words , Words'))
print(split('\W+', "Word's words Words"))

# Here ':', ' ' ,',' are not AlphaNumeric thus,
# the point where splitting occurs
print(split('\W+', 'On 12th Jan 2016, at 11:02 AM'))

# '\d+' denotes Numeric Characters or group of
# characters Splitting occurs at '12', '2016',
# '11', '02' only
print(split('\d+', 'On 12th Jan 2016, at 11:02 AM'))

"""
Output: 

['Words', 'words', 'Words']
['Word', 's', 'words', 'Words']
['On', '12th', 'Jan', '2016', 'at', '11', '02', 'AM']
['On ', 'th Jan ', ', at ', ':', ' AM']
"""
#escape
import re
print(re.escape("This is Awesome even 1 AM"))
print(re.escape("I Asked what is this [a-9], he said \t ^WoW"))
'''
Output
This\ is\ Awesome\ even\ 1\ AM
I\ Asked\ what\ is\ this\ \[a\-9\]\,\ he\ said\ \    \ \^WoW
'''

# A Python program to demonstrate working of re.match().
import re

# Lets use a regular expression to match a date string
# in the form of Month name followed by day number
regex = r"([a-zA-Z]+) (\d+)"

match = re.search(regex, "I was born on June 24")

if match != None:

    # We reach here when the expression "([a-zA-Z]+) (\d+)"
    # matches the date string.

    # This will print [14, 21), since it matches at index 14
    # and ends at 21.
    print("Match at index %s, %s" % (match.start(), match.end()))

    # We us group() method to get all the matches and
    # captured groups. The groups contain the matched values.
    # In particular:
    # match.group(0) always returns the fully matched string
    # match.group(1) match.group(2), ... return the capture
    # groups in order from left to right in the input string
    # match.group() is equivalent to match.group(0)

    # So this will print "June 24"
    print("Full match: %s" % (match.group(0)))

    # So this will print "June"
    print("Month: %s" % (match.group(1)))

    # So this will print "24"
    print("Day: %s" % (match.group(2)))

else:
    print("The regex pattern does not match.")

'''
Output
Match at index 14, 21
Full match: June 24
Month: June
Day: 24
'''
import re

s = "Welcome to GeeksForGeeks"

# here x is the match object
res = re.search(r"\bGee", s)

print(res.start())
print(res.end())
print(res.span())
#Output
#11
#14
#(11, 14)

import re

s = "Welcome to GeeksForGeeks"

# here x is the match object
res = re.search(r"\D{2} t", s)

print(res.group())
#Output
#me
#t
