people = {3: "Jim", 2: "Jack", 4: "Jane", 1: "Jill"}


 # Sort by key
#print(dict(sorted(people.items())))
{1: 'Jill', 2: 'Jack', 3: 'Jim', 4: 'Jane'}

 # Sort by value
#print(dict(sorted(people.items(), key=lambda item: item[1])))
{2: 'Jack', 4: 'Jane', 1: 'Jill', 3: 'Jim'}

Car = {'Audi':1000, 'BMW':129, 'Jaguar': 910, 'Hyundai' : 388}
lst=Car.values()
maximum=max(lst)
minimum=min(lst)
print(maximum)
print(minimum)
print(lst)
for i,j in Car.items():
    if maximum==j:
        print(i,j)
    elif minimum==j:
        print(i,j)
print(dict(sorted(Car.items(),key=lambda item: item[1])))



