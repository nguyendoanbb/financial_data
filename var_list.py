all_variables = dir()

for name in all_variables:
    if not name.startswith('_'):
        print(name)