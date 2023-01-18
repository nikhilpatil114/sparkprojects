"""
\	Used to drop the special meaning of character following it
[]	Represent a character class
^	Matches the beginning
$	Matches the end
.	Matches any character except newline
|	Means OR (Matches with any of the characters separated by it.
?	Matches zero or one occurrence
*	Any number of occurrences (including 0 occurrences)
+	One or more occurrences
{}	Indicate the number of occurrences of a preceding regex to match.
()	Enclose a group of Regex

"""

"""
[] – Square Brackets
[0, 3] is sample as [0123]
[a-c] is same as [abc]
#invert
[^0-3] means any number except 0, 1, 2, or 3
[^a-c] means any character except a, b, or c

^ – Caret
^g will check if the string starts with g such as geeks, globe, girl, g, etc.
^ge will check if the string starts with ge such as geeks, geeksforgeeks, etc.

$ – Dollar
s$ will check for the string that ends with a such as geeks, ends, s, etc.
ks$ will check for the string that ends with ks such as geeks, geeksforgeeks, ks, etc.

. – Dot
Dot(.) symbol matches only a single character except for the newline character (\n). For example –  

a.b will check for the string that contains any character at the place of the dot such as acb, acbd, abbb, etc
.. will check if the string contains at least 2 characters

| – Or

a|b will match any string that contains a or b such as acd, bcd, abcd, etc.

? – Question Mark
ab?c will be matched for the string ac, acb, dabc but will not be matched for abbc because there are two b

* – Star
Star (*) symbol matches zero or more occurrences of the regex preceding the * symbol. For example –  
ab*c will be matched for the string ac, abc, abbbc, dabc, etc. but will not be matched for abdc because b is not followed by c.

+ – Plus
Plus (+) symbol matches one or more occurrences of the regex preceding the + symbol. For example –  
ab+c will be matched for the string abc, abbc, dabc, but will not be matched for ac, abdc because there is no b in ac and b is not followed by c in abdc.
"""
"""
Special Sequence	      Description	                  Examples
\A	Matches if the string begins with the given character	\Afor 	for geeks
                                                                    for the world

\b	Matches if the word begins or ends with the given character. \b(string) will check for the beginning of the word and (string)\b will check for the ending of the word.	\bge	geeks
                                                                                                                                                                                    get

\B	It is the opposite of the \b i.e. the string should not start or end with the given regex.	\Bge	together
                                                                                                        forge 

\d	Matches any decimal digit, this is equivalent to the set class [0-9]	\d	123
                                                                                gee1

\D	Matches any non-digit character, this is equivalent to the set class [^0-9]	\D	geeks
                                                                                    geek1

\s	Matches any whitespace character.	\s	gee ks
                                            a bc a                             

\S	Matches any non-whitespace character	\S	a bd
                                                abcd

\w	Matches any alphanumeric character, this is equivalent to the class [a-zA-Z0-9_].	\w	123
                                                                                            geeKs4

\W	Matches any non-alphanumeric character.	\W	>$
                                                gee<>

\Z	Matches if the string ends with the given regex	ab\Z	abcdab
                                                            abababab
"""