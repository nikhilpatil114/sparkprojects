import re
s = 'GeeksforGeeks: A computer science portal for geeks'

match = re.search(r'portal', s)
print('Start Index:', match.start())
print('End Index:', match.end())

import re

s = 'geeks.forgeeks'
# without using \
match = re.search(r'.', s)
print(match)
# using \
match = re.search(r'\.', s)
print(match)
"""
<_sre.SRE_Match object; span=(0, 1), match='g'>
<_sre.SRE_Match object; span=(5, 6), match='.'>
"""

#re.findall()
string = """Hello my Number is 123456789 and
            my friend's number is 987654321"""
regex = '\d+'
match = re.findall(regex, string)
print(match)

#o/p--['123456789', '987654321']

#re.compile()
import re

# compile() creates regular expression
# character class [a-e],
# which is equivalent to [abcde].
# class [abcde] will match with string with
# 'a', 'b', 'c', 'd', 'e'.
p = re.compile('[a-e]')

# findall() searches for the Regular Expression
# and return a list upon finding
print(p.findall("Aye, said Mr. Gibenson Stark"))

#o/p->['e', 'a', 'd', 'b', 'e', 'a']

import re

# \d is equivalent to [0-9].
p = re.compile('\d')
print(p.findall("I went to him at 11 A.M. on 4th July 1886"))

# \d+ will match a group on [0-9], group
# of one or greater size
p = re.compile('\d+')
print(p.findall("I went to him at 11 A.M. on 4th July 1886"))

#o/p->['1', '1', '4', '1', '8', '8', '6']
#      ['11', '4', '1886']

# \w is equivalent to [a-zA-Z0-9_].
p = re.compile('\w')
print(p.findall("He said * in some_lang."))

# \w+ matches to group of alphanumeric character.
p = re.compile('\w+')
print(p.findall("I went to him at 11 A.M., he \
said *** in some_language."))

# \W matches to non alphanumeric characters.
p = re.compile('\W')
print(p.findall("he said *** in some_language."))

"""
Output: 

['H', 'e', 's', 'a', 'i', 'd', 'i', 'n', 's', 'o', 'm', 'e', '_', 'l', 'a', 'n', 'g']
['I', 'went', 'to', 'him', 'at', '11', 'A', 'M', 'he', 'said', 'in', 'some_language']
[' ', ' ', '*', '*', '*', ' ', ' ', '.']
"""