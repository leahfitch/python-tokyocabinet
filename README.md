# Introduction

`tokyocabinet` is a C Python extension that wraps the
[Tokyo Cabinet](http://fallabs.com/tokyocabinet/) database
library. The goal is to provide a complete wrapper of all library methods as
well as provide a somewhat pythonic interface.

There are actually several extensions, one for each database API. Each
extension provides one or more classes wrapping the API's functionality as well
as all the integer constants and an error class (e.g. `tokyocabinet.btree.error).

## Package Structure

### `tokyocabinet.btree`

Provides the `BTree` and `BTreeCursor` classes.

### `tokyocabinet.hash`

Provides the `Hash` class.

### `tokyocabinet.table`

Provides the `Table` and `TableQuery` classes.

For the most part, it should be easy enough to refer to the [Tokyo Cabinet
documentation](http://fallabs.com/tokyocabinet/spex-en.html) but provided below
is a basic description of the library usage, focusing on the differences from
the C api (the main difference being the use of classes and python's mapping
interface).

## Usage Examples

### Using BTree and Hash

`BTree` and `Hash` have almost identical interfaces with the exception of the
``BTreeCursor`` class. Here is an example of ``BTree`` use:

```python
>>> from tokyocabinet import btree
>>> db = btree.BTree()
>>> db.open('/tmp/test.tcb', btree.BDBOWRITER | btree.BDBOCREAT)
>>> db['loves spam'] = 'Vikings'
>>> db['parrot'] = 'not dead'
>>> len(db)
2
>>> db['parrot']
"not dead"
>>> db['apples'] = ''
>>> db['apprehend'] = ''
>>> db['arrogant'] = ''
>>> db.fwmkeys('a')
['apples', 'apprehend', 'arrogant']
>>> db.fwmkeys('ap')
['apples', 'apprehend']
>>> db['orcs']
KeyError: 'no record found'
>>> cur = db.cursor()
>>> cur.first()
>>> cur.rec()
('apples', '')
>>> cur.last()
>>> cur.next()
KeyError: 'no record found'

```

Using `tokyocabinet.hash.Hash` is essentially the same, minus the cursor bits.

### Using Table and TableQuery

The `table` API is a bit different:

```python
>>> from tokyocabinet import table
>>> db = table.Table()
>>> db.open('/tmp/test.tct', table.TDBOWRITER | table.TDBOCREAT)
>>> db['knight-a'] = {'name':'The Green Knight','strength':'mighty'}
>>> db['knight-b'] = {'name':'The Black Knight','strength':'pitiful'}
>>> db['knight-a']
{'name': 'The Green Knight', 'strength': 'mighty'}
>>> q = db.query()
>>> q.addcond('strength', table.TDBQCSTREQ, 'mighty')
>>> q.search()
['knight-a']
>>> print q.hint()
scanning the whole table
result set size: 1
leaving the natural order

>>> db.setindex('strength', table.TDBITLEXICAL)
>>> q = db.query()
>>> q.addcond('strength', table.TDBQCSTREQ, 'mighty')
>>> q.search()
['knight-a']
>>> print q.hint()
using an index: "strength" asc (STREQ)
result set size: 1
leaving the natural order
...

```

The main thing to keep in mind is that `Table`, while very powerful, is a pretty
low-level tool. It doesn't know about types so all keys and values of the dicts
stored as records must be strings. For example:

```python
...
>>> db['foo'] = {'skidoo':23}
TypeError: All values must be strings.

```

## Installation

### Mac OS X

* Install Tokyo Cabinet (if you use `brew`, it is available as a _recipe_)
* Install python-tokyocabinet

```
brew install tokyo-cabinet
pip install python-tokyocabinet
```

## Note

[FAL Labs](http://fallabs.com/) does state the Tokyo Cabinet is "surpassed" (in
every aspect) by [Kyoto Cabinet](http://fallabs.com/kyotocabinet/). You may be
using this because you have not moved available from Tokyo Cabinet, but it
should be mentioned to ensure no confusion.
