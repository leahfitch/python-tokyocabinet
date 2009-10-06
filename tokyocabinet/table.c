#include <Python.h>
#include <tctdb.h>
#include <tcutil.h>
#include <limits.h>


static PyObject *
tcmap2pydict(TCMAP *map)
{
    const char *kstr, *vstr;
    PyObject *dict, *value;
    
    dict = PyDict_New();
    
    if (dict == NULL)
    {
        PyErr_SetString(PyExc_MemoryError, "Could not allocate memory for dict.");
        return NULL;
    }
    
    tcmapiterinit(map);
    kstr = tcmapiternext2(map);
    
    while (kstr != NULL)
    {
        vstr = tcmapget2(map, kstr);
        
        if (vstr == NULL)
        {
            Py_DECREF(dict);
            PyErr_SetString(PyExc_MemoryError, "Could not allocate memory for map value.");
            return NULL;
        }
        
        value = PyString_FromString(vstr);
        
        if (value == NULL)
        {
            Py_DECREF(dict);
            PyErr_SetString(PyExc_MemoryError, "Could not allocate memory for dict value.");
            return NULL;
        }
        
        if (PyDict_SetItemString(dict, kstr, value) != 0)
        {
            Py_DECREF(value);
            Py_DECREF(dict);
            PyErr_SetString(PyExc_Exception, "Could not set dict item.");
            return NULL;
        }
        
        Py_DECREF(value);
        
        kstr = tcmapiternext2(map);
    }
    
    return dict;
}


static TCMAP *
pydict2tcmap(PyObject *dict)
{
    if (!PyDict_Check(dict))
    {
        PyErr_SetString(PyExc_TypeError, "Argument is not a dict.");
        return NULL;
    }
    
    PyObject *key, *value;
    int pos = 0;
    const char *kstr, *vstr;
    TCMAP *map;
    
    map = tcmapnew();
    
    if (map == NULL)
    {
        PyErr_SetString(PyExc_MemoryError, "Could not allocate map.");
        return NULL;
    }
    
    while (PyDict_Next(dict, &pos, &key, &value))
    {
        if (!PyString_Check(value))
        {
            tcmapdel(map);
            PyErr_SetString(PyExc_TypeError, "All values must be strings.");
            return NULL;
        }
        
        kstr = PyString_AsString(key);
        vstr = PyString_AsString(value);
        
        tcmapput2(map, kstr, vstr);
    }
    
    return map;
}


static PyObject *TableError;


static void
raise_table_error(TCTDB *db)
{
    int code = tctdbecode(db);
    const char *msg = tctdberrmsg(code);
    
    if (code == TCENOREC)
    {
        PyErr_SetString(PyExc_KeyError, msg);
    }
    else
    {
        PyErr_SetString(TableError, msg);
    }
}


static PyTypeObject TableType;
static PyTypeObject TableQueryType;

typedef struct
{
    PyObject_HEAD
    TCTDB *db;
} Table;

typedef struct
{
    PyObject_HEAD
    TDBQRY *q;
} TableQuery;



static long
TableQuery_Hash(PyObject *self)
{
    PyErr_SetString(PyExc_TypeError, "Table query objects are not hashable.");
    return -1;
}


static void
TableQuery_dealloc(TableQuery *self)
{
    if (self->q)
    {
        Py_BEGIN_ALLOW_THREADS
        tctdbqrydel(self->q);
        Py_END_ALLOW_THREADS
    }
    self->ob_type->tp_free(self);
}


static PyObject *
TableQuery_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    TableQuery *self;
    Table *pydb;
    
    self = (TableQuery *) type->tp_alloc(type, 0);
    if (!self)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate TableQuery instance.");
        return NULL;
    }
    
    if (PyArg_ParseTuple(args, "O!", &TableType, &pydb))
    {
        self->q = tctdbqrynew(pydb->db);
        if (!self->q)
        {
            raise_table_error(pydb->db);
        }
        else
        {
            return (PyObject *) self;
        }
    }
    
    TableQuery_dealloc(self);
    return NULL;
}


static PyObject *
TableQuery_addcond(TableQuery *self, PyObject *args, PyObject *kwargs)
{
    const char *name, *expr;
    name = expr = NULL;
    int op = 0;
    
    static char *kwlist[] = {"name", "op", "expr", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "si|s:addcond", kwlist, 
        &name, &op, &expr))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    tctdbqryaddcond(self->q, name, op, expr);
    Py_END_ALLOW_THREADS
    
    Py_RETURN_NONE;
}


static PyObject *
TableQuery_setorder(TableQuery *self, PyObject *args)
{
    const char *name;
    int type;
    
    if (!PyArg_ParseTuple(args, "si:setorder", &name, &type))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    tctdbqrysetorder(self->q, name, type);
    Py_END_ALLOW_THREADS
    
    Py_RETURN_NONE;
}


static PyObject *
TableQuery_setlimit(TableQuery *self, PyObject *args)
{
    int max, skip;
    max = skip = -1;
    
    if (!PyArg_ParseTuple(args, "ii:setlimit", &max, &skip))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    tctdbqrysetlimit(self->q, max, skip);
    Py_END_ALLOW_THREADS
    
    Py_RETURN_NONE;
}


static PyObject *
TableQuery_search(TableQuery *self)
{
    TCLIST *results;
    int n = 0, i=0, vsiz = 0;
    const char *vbuf;
    PyObject *pylist, *val;
    
    Py_BEGIN_ALLOW_THREADS
    results = tctdbqrysearch(self->q);
    Py_END_ALLOW_THREADS
    
    if (!results)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memory for TCLIST object");
        return NULL;
    }
    
    n = tclistnum(results);
    pylist = PyList_New(n);
    
    if (pylist)
    {
        for (i=0; i<n; i++)
        {
            vbuf = tclistval(results, i, &vsiz);
            val = PyString_FromStringAndSize(vbuf, vsiz);
            PyList_SET_ITEM(pylist, i, val);
            
            vsiz = 0;
        }
    }
    tclistdel(results);
    
    return pylist;
}


static PyObject *
TableQuery_searchout(TableQuery *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbqrysearchout(self->q);
    Py_END_ALLOW_THREADS
    
    return Py_BuildValue("i", success);
}


static PyObject *
TableQuery_count(TableQuery *self)
{
    TCLIST *results;
    int n = 0;
    
    Py_BEGIN_ALLOW_THREADS
    results = tctdbqrysearch(self->q);
    Py_END_ALLOW_THREADS
    
    if (!results)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memory for TCLIST object");
        return NULL;
    }
    
    n = tclistnum(results);
    
    return Py_BuildValue("i", n);
}


static PyMethodDef TableQuery_methods[] = 
{
    {
        "addcond", (PyCFunction) TableQuery_addcond,
        METH_VARARGS | METH_KEYWORDS,
        "Add a condition."
    },
    
    {
        "setorder", (PyCFunction) TableQuery_setorder,
        METH_VARARGS,
        "Set the column and direction to order by."
    },
    
    {
        "setlimit", (PyCFunction) TableQuery_setlimit,
        METH_VARARGS,
        "Set the offset and limit of the results."
    },
    
    {
        "search", (PyCFunction) TableQuery_search,
        METH_NOARGS,
        "Run the query. Returns the keys of matching records"
    },
    
    {
        "searchout", (PyCFunction) TableQuery_searchout,
        METH_NOARGS,
        "Remove all matching records."
    },
    
    {
        "count", (PyCFunction) TableQuery_count,
        METH_NOARGS,
        "Get a count of matching records."
    },
    
    { NULL }
};


static PyTypeObject TableQueryType = {
  PyObject_HEAD_INIT(NULL)
  0,                                           /* ob_size */
  "tokyocabinet.table.TableQuery",             /* tp_name */
  sizeof(TableQuery),                          /* tp_basicsize */
  0,                                           /* tp_itemsize */
  (destructor)TableQuery_dealloc,                   /* tp_dealloc */
  0,                                           /* tp_print */
  0,                                           /* tp_getattr */
  0,                                           /* tp_setattr */
  0,                                           /* tp_compare */
  0,                                           /* tp_repr */
  0,                                           /* tp_as_number */
  0,                                           /* tp_as_sequence */
  0,                                           /* tp_as_mapping */
  TableQuery_Hash,                             /* tp_hash  */
  0,                                           /* tp_call */
  0,                                           /* tp_str */
  0,                                           /* tp_getattro */
  0,                                           /* tp_setattro */
  0,                                           /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,    /* tp_flags */
  "Table database query",                      /* tp_doc */
  0,                                           /* tp_traverse */
  0,                                           /* tp_clear */
  0,                                           /* tp_richcompare */
  0,                                           /* tp_weaklistoffset */
  0,                                           /* tp_iter */
  0,                                           /* tp_iternext */
  TableQuery_methods,                          /* tp_methods */
  0,                                           /* tp_members */
  0,                                           /* tp_getset */
  0,                                           /* tp_base */
  0,                                           /* tp_dict */
  0,                                           /* tp_descr_get */
  0,                                           /* tp_descr_set */
  0,                                           /* tp_dictoffset */
  0,                                           /* tp_init */
  0,                                           /* tp_alloc */
  TableQuery_new,                              /* tp_new */
};



static long
Table_Hash(PyObject *self)
{
    PyErr_SetString(PyExc_TypeError, "Table objects are not hashable.");
    return -1;
}


static void
Table_dealloc(Table *self)
{
    if (self->db)
    {
        Py_BEGIN_ALLOW_THREADS
        tctdbdel(self->db);
        Py_END_ALLOW_THREADS
    }
    self->ob_type->tp_free(self);
}


static PyObject *
Table_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    Table *self;
    
    self = (Table *) type->tp_alloc(type, 0);
    if (!self)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate Table instance.");
        return NULL;
    }
    
    self->db = tctdbnew();
    if (!self->db)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate TCTDB instance.");
        return NULL;
    }
    
    return (PyObject *) self;
}


static PyObject *
Table_setmutex(Table *self)
{
    bool success = 0;
    Py_BEGIN_ALLOW_THREADS
    success = tctdbsetmutex(self->db);
    Py_END_ALLOW_THREADS
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_tune(Table *self, PyObject *args, PyObject *kwargs)
{
    bool success = 0;
    PY_LONG_LONG bnum = 0;
    short apow, fpow;
    apow = fpow = -1;
    unsigned char opts = 0;
    
    static char *kwlist[] = {"bnum", "apow", "fpow", "opts", NULL };
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|LhhB:tune", kwlist, 
        &bnum, &apow, &fpow, &opts))
    {
        return NULL;
    }
    
    if (apow < SCHAR_MIN || apow > SCHAR_MAX ||
        fpow < SCHAR_MIN || fpow > SCHAR_MAX)
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbtune(self->db, bnum, apow, fpow, opts);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_setcache(Table *self, PyObject *args, PyObject *kwargs)
{
    bool success = 0;
    int rcnum, lcnum, ncnum;
    rcnum = lcnum = ncnum = 0;
    
    static char *kwlist[] = {"rcnum", "lcnum", "ncnum", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ii", kwlist, 
        &rcnum, &lcnum, &ncnum))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbsetcache(self->db, rcnum, lcnum, ncnum);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_setxmsize(Table *self, PyObject *args)
{
    bool success = 0;
    PY_LONG_LONG xmsiz = 0;
    
    if (!PyArg_ParseTuple(args, "L", &xmsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbsetxmsiz(self->db, xmsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_setdfunit(Table *self, PyObject *args)
{
    bool success = 0;
    int dfunit = 0;
    
    if (!PyArg_ParseTuple(args, "i", &dfunit))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbsetdfunit(self->db, dfunit);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_open(Table *self, PyObject *args, PyObject *kwargs)
{
    int omode = TDBOWRITER | TDBOCREAT;
    char *path = NULL;
    static char *kwlist[] = { "path", "omode", NULL };
    
    if (PyArg_ParseTupleAndKeywords(args, kwargs, "s|i:open", kwlist, &path, &omode))
    {
        bool success = 0;
        Py_BEGIN_ALLOW_THREADS
        success = tctdbopen(self->db, path, omode);
        Py_END_ALLOW_THREADS
        if (success)
        {
            Py_RETURN_NONE;
        }
        raise_table_error(self->db);
    }
    return NULL;
}


static PyObject *
Table_close(Table *self)
{
    bool success = 0;
    Py_BEGIN_ALLOW_THREADS
    success = tctdbclose(self->db);
    Py_END_ALLOW_THREADS
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_put(Table *self, PyObject *args)
{
    bool success;
    char *kbuf;
    int ksiz;
    TCMAP *cols;
    PyObject *dict;
    
    if (!PyArg_ParseTuple(args, "s#O:put", &kbuf, &ksiz, &dict))
    {
        return NULL;
    }
    
    cols = pydict2tcmap(dict);
    
    if (cols == NULL)
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbput(self->db, kbuf, ksiz, cols);
    Py_END_ALLOW_THREADS
    
    tcmapdel(cols);
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Table_putkeep(Table *self, PyObject *args)
{
    bool success;
    char *kbuf;
    int ksiz;
    TCMAP *cols;
    PyObject *dict;
    
    if (!PyArg_ParseTuple(args, "s#O:put", &kbuf, &ksiz, &dict))
    {
        return NULL;
    }
    
    cols = pydict2tcmap(dict);
    
    if (cols == NULL)
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbputkeep(self->db, kbuf, ksiz, cols);
    Py_END_ALLOW_THREADS
    
    tcmapdel(cols);
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_putcat(Table *self, PyObject *args)
{
    bool success;
    char *kbuf;
    int ksiz;
    TCMAP *cols;
    PyObject *dict;
    
    if (!PyArg_ParseTuple(args, "s#O:put", &kbuf, &ksiz, &dict))
    {
        return NULL;
    }
    
    cols = pydict2tcmap(dict);
    
    if (cols == NULL)
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbputcat(self->db, kbuf, ksiz, cols);
    Py_END_ALLOW_THREADS
    
    tcmapdel(cols);
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_out(Table *self, PyObject *args)
{
    bool success;
    char *kbuf;
    int ksiz;
    
    if (!PyArg_ParseTuple(args, "s#:out", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbout(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_get(Table *self, PyObject *args)
{
    char *kbuf;
    int ksiz;
    TCMAP *cols;
    PyObject *value;
    
    if (!PyArg_ParseTuple(args, "s#:get", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    cols = tctdbget(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    if (!cols)
    {
        Py_RETURN_NONE;
    }
    
    value = tcmap2pydict(cols);
    tcmapdel(cols);
    
    if (!value)
    {
        return NULL;
    }
    
    return value;
}


static PyObject *
Table_vsiz(Table *self, PyObject *args)
{
    char *kbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#:vsiz", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    vsiz = tctdbvsiz(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    return Py_BuildValue("i", vsiz);
}


static PyObject *
Table_fwmkeys(Table *self, PyObject *args, PyObject *kwargs)
{
    char *pbuf;
    int psiz, i, n;
    int max = -1;
    PyObject *pylist;
    TCLIST *list;
    
    static char *kwlist[] = {"prefix", "max", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#|i:fwmkeys", kwlist,
        &pbuf, &psiz, &max))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    list = tctdbfwmkeys(self->db, pbuf, psiz, max);
    Py_END_ALLOW_THREADS
    
    if (!list)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memory for TCLIST object");
        return NULL;
    }
    
    n = tclistnum(list);
    pylist = PyList_New(n);
    if (pylist)
    {
        for (i=0; i<n; i++)
        {
            int vsiz;
            const char *vbuf;
            PyObject *value;
            
            vbuf = tclistval(list, i, &vsiz);
            value = PyString_FromStringAndSize(vbuf, vsiz);
            PyList_SET_ITEM(pylist, i, value);
        }
    }
    tclistdel(list);
    
    return pylist;
}


static PyObject *
Table_addint(Table *self, PyObject *args)
{
    char *kbuf;
    int ksiz, num, result;
    
    if (!PyArg_ParseTuple(args, "s#i:addint", &kbuf, &ksiz, &num))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    result = tctdbaddint(self->db, kbuf, ksiz, num);
    Py_END_ALLOW_THREADS
    
    return PyInt_FromLong((long) result);
}


static PyObject *
Table_adddouble(Table *self, PyObject *args)
{
    char *kbuf;
    int ksiz;
    double num, result;
    
    if (!PyArg_ParseTuple(args, "s#d:adddouble", &kbuf, &ksiz, &num))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    result = tctdbadddouble(self->db, kbuf, ksiz, num);
    Py_END_ALLOW_THREADS
    
    return PyFloat_FromDouble(result);
}


static PyObject *
Table_sync(Table *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbsync(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Table_optimize(Table *self, PyObject *args, PyObject *kwargs)
{
    bool success = 0;
    PY_LONG_LONG bnum = 0;
    short apow, fpow;
    apow = fpow = -1;
    unsigned char opts = UCHAR_MAX;
    
    static char *kwlist[] = {"bnum", "apow", "fpow", "opts", NULL };
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|LhhB:optimize", kwlist, 
        &bnum, &apow, &fpow, &opts))
    {
        return NULL;
    }
    
    if (apow < SCHAR_MIN || apow > SCHAR_MAX ||
        fpow < SCHAR_MIN || fpow > SCHAR_MAX)
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdboptimize(self->db, bnum, apow, fpow, opts);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_vanish(Table *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbvanish(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Table_copy(Table *self, PyObject *args)
{
    bool success;
    char *path;
    
    if (!PyArg_ParseTuple(args, "s", &path))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbcopy(self->db, path);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Table_tranbegin(Table *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbtranbegin(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Table_trancommit(Table *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbtrancommit(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Table_tranabort(Table *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbtranabort(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Table_path(Table *self)
{
    const char *path;
    
    Py_BEGIN_ALLOW_THREADS
    path = tctdbpath(self->db);
    Py_END_ALLOW_THREADS
    
    if (!path)
    {
        Py_RETURN_NONE;
    }
    
    return PyString_FromString(path);
}


static PyObject *
Table_rnum(Table *self)
{
    uint64_t rnum;
    
    Py_BEGIN_ALLOW_THREADS
    rnum = tctdbrnum(self->db);
    Py_END_ALLOW_THREADS
    
    return PyLong_FromLongLong(rnum);
}


static PyObject *
Table_fsiz(Table *self)
{
    uint64_t fsiz;
    
    Py_BEGIN_ALLOW_THREADS
    fsiz = tctdbfsiz(self->db);
    Py_END_ALLOW_THREADS
    
    return PyLong_FromLongLong(fsiz);
}


static PyObject *
Table_setindex(Table *self, PyObject *args)
{
    bool success = 0;
    const char *name;
    int opts;
    
    if (!PyArg_ParseTuple(args, "si:setindex", &name, &opts))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbsetindex(self->db, name, opts);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_table_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Table_genuid(Table *self)
{
    int64_t id = 0;
    
    Py_BEGIN_ALLOW_THREADS
    id = tctdbgenuid(self->db);
    Py_END_ALLOW_THREADS
    
    if (id < 0)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    return Py_BuildValue("L", id);
}


static PyObject *
Table_query(Table *self)
{
    PyObject *query;
    PyObject *args;
    
    args = Py_BuildValue("(O)", self);
    query = TableQuery_new(&TableQueryType, args, NULL);
    Py_DECREF(args);
    
    if (!query)
    {
        raise_table_error(self->db);
        return NULL;
    }
    
    return query;
}


static PyObject *
Table_metasearch(Table *self, PyObject *args)
{
    TDBQRY **queries;
    TableQuery *query;
    TCLIST *results;
    int n = 0, i=0, vsiz = 0, type = 0;
    const char *vbuf;
    PyObject *pyresults, *val, *pyqueries, *item;
    
    if (!PyArg_ParseTuple(args, "Oi:metasearch", &pyqueries, &type))
    {
        return NULL;
    }
    
    if (!PyList_Check(pyqueries))
    {
        return NULL;
    }
    
    n = PyList_Size(pyqueries);
    
    if (n == 0)
    {
        pyresults = PyList_New(0);
        return pyresults;
    }
    
    queries = (TDBQRY **) malloc(sizeof(TDBQRY *) * n);
    
    if (queries == NULL)
    {
        PyErr_SetString(PyExc_MemoryError, "Could not allocate queries list.");
        return NULL;
    }
    
    for (i=0; i<n; i++)
    {
        item = PyList_GetItem(pyqueries, i);
        
        if (!PyObject_TypeCheck(item, &TableQueryType))
        {
            PyErr_SetString(PyExc_TypeError, "Expected a list of table query objects.");
            free(queries);
            return NULL;
        }
        
        query = (TableQuery *) item;
        queries[i] = query->q;
    }
    
    Py_BEGIN_ALLOW_THREADS
    results = tctdbmetasearch(queries, n, type);
    Py_END_ALLOW_THREADS
    
    if (!results)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memory for TCLIST object");
        return NULL;
    }
    
    n = tclistnum(results);
    pyresults = PyList_New(n);
    
    if (pyresults)
    {
        for (i=0; i<n; i++)
        {
            vbuf = tclistval(results, i, &vsiz);
            val = PyString_FromStringAndSize(vbuf, vsiz);
            PyList_SET_ITEM(pyresults, i, val);
            
            vsiz = 0;
        }
    }
    tclistdel(results);
    
    return pyresults;
}


static Py_ssize_t
Table_length(Table *self)
{
    uint64_t rnum;
    
    Py_BEGIN_ALLOW_THREADS
    rnum = tctdbrnum(self->db);
    Py_END_ALLOW_THREADS
    
    return (Py_ssize_t) rnum;
}


static PyObject *
Table_subscript(Table *self, PyObject *key)
{
    char *kbuf;
    int ksiz;
    TCMAP *cols;
    PyObject *value;
    
    if (!PyString_Check(key))
    {
        PyErr_SetString(PyExc_TypeError, "Expected key to be a string.");
    }
    
    PyString_AsStringAndSize(key, &kbuf, &ksiz);
    
    Py_BEGIN_ALLOW_THREADS
    cols = tctdbget(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    if (!cols)
    {
        Py_RETURN_NONE;
    }
    
    value = tcmap2pydict(cols);
    tcmapdel(cols);
    
    if (!value)
    {
        return NULL;
    }
    
    return value;
}


static int
Table_ass_subscript(Table *self, PyObject *key, PyObject *value)
{
    bool success;
    char *kbuf;
    int ksiz;
    TCMAP *cols;
    
    if (!PyString_Check(key))
    {
        PyErr_SetString(PyExc_TypeError, "Expected key to be a string.");
    }
    
    PyString_AsStringAndSize(key, &kbuf, &ksiz);
    
    cols = pydict2tcmap(value);
    
    if (cols == NULL)
    {
        return -1;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tctdbput(self->db, kbuf, ksiz, cols);
    Py_END_ALLOW_THREADS
    
    tcmapdel(cols);
    
    if (!success)
    {
        raise_table_error(self->db);
        return -1;
    }
    
    return 0;
}


static PyMappingMethods Table_as_mapping = 
{
    (lenfunc) Table_length,
    (binaryfunc) Table_subscript,
    (objobjargproc) Table_ass_subscript
};


static int
Table_contains(Table *self, PyObject *value)
{
    char *kbuf;
    Py_ssize_t ksiz;
    int vsiz;
    
    if (!PyString_Check(value))
    {
        PyErr_SetString(PyExc_ValueError, "Expected value to be a string");
        return -1;
    }
    
    PyString_AsStringAndSize(value, &kbuf, &ksiz);
    if (!kbuf)
    {
        return -1;
    }
    
    Py_BEGIN_ALLOW_THREADS
    vsiz = tctdbvsiz(self->db, kbuf, (int) ksiz);
    Py_END_ALLOW_THREADS
    
    return vsiz != -1;
}


static PySequenceMethods Table_as_sequence = 
{
    0,                             /* sq_length */
    0,                             /* sq_concat */
    0,                             /* sq_repeat */
    0,                             /* sq_item */
    0,                             /* sq_slice */
    0,                             /* sq_ass_item */
    0,                             /* sq_ass_slice */
    (objobjproc) Table_contains,   /* sq_contains */
    0,                             /* sq_inplace_concat */
    0                              /* sq_inplace_repeat */
};


static PyMethodDef Table_methods[] = 
{
    {
        "setmutex", (PyCFunction) Table_setmutex,
        METH_NOARGS,
        "Set the mutual exclusion control of the database for threading."
    },
    
    {
        "tune", (PyCFunction) Table_tune,
        METH_VARARGS | METH_KEYWORDS,
        "Set tuning parameters."
    },
    
    {
        "setcache", (PyCFunction) Table_setcache,
        METH_VARARGS | METH_KEYWORDS,
        "Set cache parameters."
    },
    
    {
        "setxmsize", (PyCFunction) Table_setxmsize,
        METH_VARARGS | METH_KEYWORDS,
        "Set size of extra mapped memory."
    },
    
    {
        "setdfunit", (PyCFunction) Table_setdfunit,
        METH_VARARGS,
        "Set the unit step number of auto defragmentation."
    },
    
    {
        "open", (PyCFunction) Table_open, 
        METH_VARARGS | METH_KEYWORDS,
        "Open the database at the given path in the given mode"
    },
    
    {
        "close", (PyCFunction) Table_close,
        METH_NOARGS,
        "Close the database"
    },
    
    {
        "put", (PyCFunction) Table_put,
        METH_VARARGS,
        "Store a record. Overwrite existing record."
    },
    
    {
        "putkeep", (PyCFunction) Table_putkeep,
        METH_VARARGS,
        "Store a record. Don't overwrite an existing record."
    },
    
    {
        "putcat", (PyCFunction) Table_putcat,
        METH_VARARGS,
        "Concatenate value on the end of a record. Creates the record if it doesn't exist."
    },
    
    {
        "out", (PyCFunction) Table_out,
        METH_VARARGS,
        "Remove a record. If there are duplicates only the first is removed."
    },
    
    {
        "get", (PyCFunction) Table_get,
        METH_VARARGS,
        "Retrieve a record. If none is found None or the supplied default value is returned."
    },
    
    {
        "vsiz", (PyCFunction) Table_vsiz,
        METH_VARARGS,
        "Get the size of the of the record for key."
    },
    
    {
        "fwmkeys", (PyCFunction) Table_fwmkeys,
        METH_VARARGS | METH_KEYWORDS,
        "Get a list of of keys that match the given prefix."
    },
    
    {
        "addint", (PyCFunction) Table_addint,
        METH_VARARGS,
        "Add an integer to the selected record."
    },
    
    {
        "adddouble", (PyCFunction) Table_adddouble,
        METH_VARARGS,
        "Add a double to the selected record."
    },
    
    {
        "sync", (PyCFunction) Table_sync,
        METH_NOARGS,
        "Sync data with the disk device."
    },
    
    {
        "optimize", (PyCFunction) Table_optimize,
        METH_VARARGS,
        "Optimize a fragmented database."
    },
    
    {
        "vanish", (PyCFunction) Table_vanish,
        METH_NOARGS,
        "Remove all records from the database."
    },
    
    {
        "copy", (PyCFunction) Table_copy,
        METH_VARARGS,
        "Copy the database to a new file."
    },
    
    {
        "tranbegin", (PyCFunction) Table_tranbegin,
        METH_NOARGS,
        "Start transaction."
    },
    
    {
        "trancommit", (PyCFunction) Table_trancommit,
        METH_NOARGS,
        "Commit transaction."
    },
    
    {
        "tranabort", (PyCFunction) Table_tranabort,
        METH_NOARGS,
        "Abort transaction."
    },
    
    {
        "path", (PyCFunction) Table_path,
        METH_NOARGS,
        "Get the path of the database file or None if unconnected."
    },
    
    {
        "rnum", (PyCFunction) Table_rnum,
        METH_NOARGS,
        "Get the number of records in the database."
    },
    
    {
        "fsiz", (PyCFunction) Table_fsiz,
        METH_NOARGS,
        "Get the size of the database in bytes."
    },
    
    {
        "setindex", (PyCFunction) Table_setindex,
        METH_VARARGS,
        "Set an index on a column."
    },
    
    {
        "genuid", (PyCFunction) Table_genuid,
        METH_VARARGS,
        "Generate a unique record id."
    },
    
    {
        "query", (PyCFunction) Table_query,
        METH_NOARGS,
        "Get a query object for this database."
    },
    
    {
        "metasearch", (PyCFunction) Table_metasearch,
        METH_VARARGS,
        "Use multiple query objects and a set operation to retrieve records."
    },
    
    { NULL }
};


static PyTypeObject TableType = {
  PyObject_HEAD_INIT(NULL)
  0,                                           /* ob_size */
  "tokyocabinet.table.Table",                  /* tp_name */
  sizeof(Table),                               /* tp_basicsize */
  0,                                           /* tp_itemsize */
  (destructor)Table_dealloc,                   /* tp_dealloc */
  0,                                           /* tp_print */
  0,                                           /* tp_getattr */
  0,                                           /* tp_setattr */
  0,                                           /* tp_compare */
  0,                                           /* tp_repr */
  0,                                           /* tp_as_number */
  &Table_as_sequence,                          /* tp_as_sequence */
  &Table_as_mapping,                           /* tp_as_mapping */
  Table_Hash,                                  /* tp_hash  */
  0,                                           /* tp_call */
  0,                                           /* tp_str */
  0,                                           /* tp_getattro */
  0,                                           /* tp_setattro */
  0,                                           /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,    /* tp_flags */
  "Table database",                            /* tp_doc */
  0,                                           /* tp_traverse */
  0,                                           /* tp_clear */
  0,                                           /* tp_richcompare */
  0,                                           /* tp_weaklistoffset */
  0,                                           /* tp_iter */
  0,                                           /* tp_iternext */
  Table_methods,                               /* tp_methods */
  0,                                           /* tp_members */
  0,                                           /* tp_getset */
  0,                                           /* tp_base */
  0,                                           /* tp_dict */
  0,                                           /* tp_descr_get */
  0,                                           /* tp_descr_set */
  0,                                           /* tp_dictoffset */
  0,                                           /* tp_init */
  0,                                           /* tp_alloc */
  Table_new,                                   /* tp_new */
};


#define ADD_INT_CONSTANT(module, CONSTANT) PyModule_AddIntConstant(module, #CONSTANT, CONSTANT)

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
inittable(void)
{
    PyObject *m;
    
    m = Py_InitModule3(
            "tokyocabinet.table", NULL, 
            "Tokyo cabinet Table database wrapper"
    );
    
    if (!m)
    {
        return;
    }
    
    TableError = PyErr_NewException("tokyocabinet.table.error", NULL, NULL);
    Py_INCREF(TableError);
    PyModule_AddObject(m, "error", TableError);
    
    if (PyType_Ready(&TableType) < 0)
    {
        return;
    }
    
    if (PyType_Ready(&TableQueryType) < 0)
    {
        return;
    }
    
    
    Py_INCREF(&TableType);
    PyModule_AddObject(m, "Table", (PyObject *) &TableType);
    
    Py_INCREF(&TableQueryType);
    PyModule_AddObject(m, "TableQuery", (PyObject *) &TableQueryType);
    
    ADD_INT_CONSTANT(m, TDBOREADER);
    ADD_INT_CONSTANT(m, TDBOWRITER);
    ADD_INT_CONSTANT(m, TDBOCREAT);
    ADD_INT_CONSTANT(m, TDBOTRUNC);
    ADD_INT_CONSTANT(m, TDBOTSYNC);
    ADD_INT_CONSTANT(m, TDBONOLCK);
    ADD_INT_CONSTANT(m, TDBOLCKNB);
    
    ADD_INT_CONSTANT(m, TDBTLARGE);
    ADD_INT_CONSTANT(m, TDBTDEFLATE);
    ADD_INT_CONSTANT(m, TDBTBZIP);
    ADD_INT_CONSTANT(m, TDBTTCBS);
    
    ADD_INT_CONSTANT(m, TDBITLEXICAL);
    ADD_INT_CONSTANT(m, TDBITDECIMAL);
    ADD_INT_CONSTANT(m, TDBITTOKEN);
    ADD_INT_CONSTANT(m, TDBITQGRAM);
    ADD_INT_CONSTANT(m, TDBITOPT);
    ADD_INT_CONSTANT(m, TDBITVOID);
    ADD_INT_CONSTANT(m, TDBITKEEP);
    
    ADD_INT_CONSTANT(m, TDBQCSTREQ);
    ADD_INT_CONSTANT(m, TDBQCSTRINC);
    ADD_INT_CONSTANT(m, TDBQCSTRBW);
    ADD_INT_CONSTANT(m, TDBQCSTREW);
    ADD_INT_CONSTANT(m, TDBQCSTRAND);
    ADD_INT_CONSTANT(m, TDBQCSTROR);
    ADD_INT_CONSTANT(m, TDBQCSTROREQ);
    ADD_INT_CONSTANT(m, TDBQCSTRRX);
    ADD_INT_CONSTANT(m, TDBQCNUMEQ);
    ADD_INT_CONSTANT(m, TDBQCNUMGT);
    ADD_INT_CONSTANT(m, TDBQCNUMGE);
    ADD_INT_CONSTANT(m, TDBQCNUMLT);
    ADD_INT_CONSTANT(m, TDBQCNUMLE);
    ADD_INT_CONSTANT(m, TDBQCNUMBT);
    ADD_INT_CONSTANT(m, TDBQCNUMOREQ);
    ADD_INT_CONSTANT(m, TDBQCFTSPH);
    ADD_INT_CONSTANT(m, TDBQCFTSAND);
    ADD_INT_CONSTANT(m, TDBQCFTSOR);
    ADD_INT_CONSTANT(m, TDBQCFTSEX);
    ADD_INT_CONSTANT(m, TDBQCNEGATE);
    ADD_INT_CONSTANT(m, TDBQCNOIDX);
    
    ADD_INT_CONSTANT(m, TDBQOSTRASC);
    ADD_INT_CONSTANT(m, TDBQOSTRDESC);
    ADD_INT_CONSTANT(m, TDBQONUMASC);
    ADD_INT_CONSTANT(m, TDBQONUMDESC);
    
    ADD_INT_CONSTANT(m, TDBMSUNION);
    ADD_INT_CONSTANT(m, TDBMSISECT);
    ADD_INT_CONSTANT(m, TDBMSDIFF);
}
