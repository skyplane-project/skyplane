# Skyplane Documentation

Install packages for docs: 
```
pip install -r docs/requirements.txt
pip install sphinx-autobuild
```

Run `cd docs/` to make sure you're in the documentation directory. Then, build the docs with: 
```
sphinx-autobuild -b html -d build/doctrees . build/html
```
which will output a localhost port where you can view the docs. 
