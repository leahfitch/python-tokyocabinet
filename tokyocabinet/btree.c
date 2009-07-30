#include <Python.h>
#include <tcbdb.h>
#include <tcutil.h>
#include <limits.h>


static PyObject *BTreeError;


static void
raise_btree_error(TCBDB *db)
{
    int code = tcbdbecode(db);
    const char *msg = tcbdberrmsg(code);
    
    if (code == TCENOREC)
    {
        PyErr_SetString(PyExc_KeyError, msg);
    }
    else
    {
        PyErr_SetString(BTreeError, msg);
    }
}


static PyTypeObject BTreeType;


typedef struct
{
    PyObject_HEAD
    TCBDB *db;
    PyObject *cmp;
    PyObject *cmpop;
} BTree;


typedef struct
{
    PyObject_HEAD
    BTree *pydb;
    BDBCUR *cur;
} BTreeCursor;


static long
BTreeCursor_Hash(PyObject *self)
{
    PyErr_SetString(PyExc_TypeError, "BTreeCursor objects are not hashable.");
    return -1;
}


static void
BTreeCursor_dealloc(BTreeCursor *self)
{
    Py_XDECREF((PyObject *) self->pydb);
    if (self->cur)
    {
        Py_BEGIN_ALLOW_THREADS
        tcbdbcurdel(self->cur);
        Py_END_ALLOW_THREADS
    }
    self->ob_type->tp_free(self);
}


static PyObject *
BTreeCursor_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    BTreeCursor *self;
    BTree *pydb;
    
    self = (BTreeCursor *) type->tp_alloc(type, 0);
    if (!self)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate BTreeCursor instance.");
        return NULL;
    }
    
    if (PyArg_ParseTuple(args, "O!", &BTreeType, &pydb))
    {
        Py_INCREF(pydb);
        self->pydb = pydb;
        
        self->cur = tcbdbcurnew(self->pydb->db);
        if (!self->cur)
        {
            raise_btree_error(self->pydb->db);
        }
        else
        {
            return (PyObject *) self;
        }
    }
    
    BTreeCursor_dealloc(self);
    return NULL;
}


static PyObject *
BTreeCursor_first(BTreeCursor *self)
{
    bool success;
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcurfirst(self->cur);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTreeCursor_last(BTreeCursor *self)
{
    bool success;
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcurlast(self->cur);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTreeCursor_jump(BTreeCursor *self, PyObject *args)
{
    bool success;
    char *kbuf;
    int ksiz;
    
    if (!PyArg_ParseTuple(args, "s#:jump", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcurjump(self->cur, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTreeCursor_prev(BTreeCursor *self)
{
    bool success;
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcurprev(self->cur);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTreeCursor_next(BTreeCursor *self)
{
    bool success;
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcurnext(self->cur);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTreeCursor_put(BTreeCursor *self, PyObject *args)
{
    bool success;
    char *vbuf;
    int vsiz, cpmode;
    
    if (!PyArg_ParseTuple(args, "s#i:jump", &vbuf, &vsiz, &cpmode))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcurput(self->cur, vbuf, vsiz, cpmode);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTreeCursor_out(BTreeCursor *self)
{
    bool success;
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcurout(self->cur);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTreeCursor_key(BTreeCursor *self)
{
    PyObject *key;
    char *kbuf;
    int ksiz;
    
    Py_BEGIN_ALLOW_THREADS
    kbuf = tcbdbcurkey(self->cur, &ksiz);
    Py_END_ALLOW_THREADS
    
    if (!kbuf)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    
    key = PyString_FromStringAndSize(kbuf, ksiz);
    free(kbuf);
    
    if (!key)
    {
        return NULL;
    }
    
    return key;
}


static PyObject *
BTreeCursor_val(BTreeCursor *self)
{
    PyObject *val;
    char *vbuf;
    int vsiz;
    
    Py_BEGIN_ALLOW_THREADS
    vbuf = tcbdbcurval(self->cur, &vsiz);
    Py_END_ALLOW_THREADS
    
    if (!vbuf)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    
    val = PyString_FromStringAndSize(vbuf, vsiz);
    free(vbuf);
    
    if (!val)
    {
        return NULL;
    }
    
    return val;
}


static PyObject *
BTreeCursor_rec(BTreeCursor *self)
{
    bool success;
    PyObject *tuple;
    TCXSTR *key, *val;
    
    key = tcxstrnew();
    val = tcxstrnew();
    
    if (!key || !val)
    {
        PyErr_SetString(PyExc_MemoryError, "Could not allocate TCXSTR object");
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcurrec(self->cur, key, val);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->pydb->db);
        return NULL;
    }
    
    tuple = Py_BuildValue("(s#s#)", tcxstrptr(key), tcxstrsize(key),
        tcxstrptr(val), tcxstrsize(val));
    
    tcxstrdel(key); tcxstrdel(val);
    
    if (!tuple)
    {
        return NULL;
    }
    
    return tuple;
}


static PyMethodDef BTreeCursor_methods[] = 
{
    {
        "first", (PyCFunction) BTreeCursor_first,
        METH_NOARGS,
        "Set the cursor to the first record."
    },
    
    {
        "last", (PyCFunction) BTreeCursor_last,
        METH_NOARGS,
        "Set the cursor to the last record."
    },
    
    {
        "jump", (PyCFunction) BTreeCursor_jump,
        METH_VARARGS,
        "Set the cursor to the first record matching the key."
    },
    
    {
        "prev", (PyCFunction) BTreeCursor_prev,
        METH_NOARGS,
        "Set the cursor to the previous record."
    },
    
    {
        "next", (PyCFunction) BTreeCursor_next,
        METH_NOARGS,
        "Set the cursor to the next record."
    },
    
    {
        "put", (PyCFunction) BTreeCursor_put,
        METH_VARARGS,
        "Insert a record according to insertion flags."
    },
    
    {
        "out", (PyCFunction) BTreeCursor_out,
        METH_NOARGS,
        "Remove the current record and set the cursor to the next record, if available."
    },
    
    {
        "key", (PyCFunction) BTreeCursor_key,
        METH_NOARGS,
        "Get the key of the record at the current position."
    },
    
    {
        "val", (PyCFunction) BTreeCursor_val,
        METH_NOARGS,
        "Get the value of the record at the current position."
    },
    
    {
        "rec", (PyCFunction) BTreeCursor_rec,
        METH_NOARGS,
        "Get the (key, value) tuple of the record at the current position."
    },
    
    { NULL }
};


static PyTypeObject BTreeCursorType = {
  PyObject_HEAD_INIT(NULL)
  0,                                           /* ob_size */
  "tokyocabinet.btree.BTreeCursor",            /* tp_name */
  sizeof(BTreeCursor),                         /* tp_basicsize */
  0,                                           /* tp_itemsize */
  (destructor)BTreeCursor_dealloc,             /* tp_dealloc */
  0,                                           /* tp_print */
  0,                                           /* tp_getattr */
  0,                                           /* tp_setattr */
  0,                                           /* tp_compare */
  0,                                           /* tp_repr */
  0,                                           /* tp_as_number */
  0,                                           /* tp_as_sequence */
  0,                                           /* tp_as_mapping */
  BTreeCursor_Hash,                            /* tp_hash  */
  0,                                           /* tp_call */
  0,                                           /* tp_str */
  0,                                           /* tp_getattro */
  0,                                           /* tp_setattro */
  0,                                           /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,    /* tp_flags */
  "Cursor for a BTree database",               /* tp_doc */
  0,                                           /* tp_traverse */
  0,                                           /* tp_clear */
  0,                                           /* tp_richcompare */
  0,                                           /* tp_weaklistoffset */
  0,                                           /* tp_iter */
  0,                                           /* tp_iternext */
  BTreeCursor_methods,                         /* tp_methods */
  0,                                           /* tp_members */
  0,                                           /* tp_getset */
  0,                                           /* tp_base */
  0,                                           /* tp_dict */
  0,                                           /* tp_descr_get */
  0,                                           /* tp_descr_set */
  0,                                           /* tp_dictoffset */
  0,                                           /* tp_init */
  0,                                           /* tp_alloc */
  BTreeCursor_new,                             /* tp_new */
};


static long
BTree_Hash(PyObject *self)
{
    PyErr_SetString(PyExc_TypeError, "BTree objects are not hashable.");
    return -1;
}


static void
BTree_dealloc(BTree *self)
{
    Py_XDECREF(self->cmp);
    Py_XDECREF(self->cmpop);
    if (self->db)
    {
        Py_BEGIN_ALLOW_THREADS
        tcbdbdel(self->db);
        Py_END_ALLOW_THREADS
    }
    self->ob_type->tp_free(self);
}


static PyObject *
BTree_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    BTree *self;
    
    self = (BTree *) type->tp_alloc(type, 0);
    if (!self)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate BTree instance.");
        return NULL;
    }
    
    self->cmp = self->cmpop = NULL;
    
    self->db = tcbdbnew();
    if (!self->db)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate TCBDB instance.");
        return NULL;
    }
    
    int omode = BDBOWRITER | BDBOCREAT;
    char *path = NULL;
    static char *kwlist[] = { "path", "omode", NULL };
    
    if (PyArg_ParseTupleAndKeywords(args, kwargs, "|si", kwlist, &path, &omode))
    {
        if (path)
        {
            bool success = 0;
            Py_BEGIN_ALLOW_THREADS
            success = tcbdbopen(self->db, path, omode);
            Py_END_ALLOW_THREADS
            if (success)
            {
                return (PyObject *) self;
            }
            raise_btree_error(self->db);
        }
        else
        {
            return (PyObject *) self;
        }
    }
    
    BTree_dealloc(self);
    return NULL;
}


static PyObject *
BTree_setmutex(BTree *self)
{
    bool success = 0;
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbsetmutex(self->db);
    Py_END_ALLOW_THREADS
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static int
BTree_cmpfunc(const char *abuf, int asiz, const char *bbuf, int bsiz, BTree *self)
{
    int retval = 0;
    PyObject *args, *result;
    PyGILState_STATE gilstate;
    
    args = Py_BuildValue("(s#s#O)", abuf, asiz, bbuf, bsiz, self->cmpop);
    if (!args)
    {
        /* PyThreadState_SetAsyncExc in main thread ?? */
        return 0;
    }
    
    gilstate = PyGILState_Ensure();
    
    result = PyEval_CallObject(self->cmp, args);
    Py_DECREF(args);
    
    if (!result)
    {
        /* Again...PyThreadState_SetAsyncExc? */
        PyGILState_Release(gilstate);
        return 0;
    }
    
    retval = PyLong_AsLong(result);
    Py_DECREF(result);
    PyGILState_Release(gilstate);
    return retval;
}


static BDBCMP *BTree_builtin_cmp[] =
{
    NULL,
    (BDBCMP *) &tcbdbcmplexical,
    (BDBCMP *) &tcbdbcmpdecimal,
    (BDBCMP *) &tcbdbcmpint32,
    (BDBCMP *) &tcbdbcmpint64
};

static PyObject *
BTree_setcmpfunc(BTree *self, PyObject *args, PyObject *kwargs)
{
    int success = 0;
    int builtin = -1;
    PyObject *cmp, *cmpop;
    static char *kwlist[] = { "cmp", "cmpop", "builtin", NULL };
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOi:setcmpfunc", 
        kwlist, &cmp, &cmpop, &builtin))
    {
        return NULL;
    }
    
    if (builtin > 0)
    {
        if (builtin > 4)
        {
            PyErr_SetString(BTreeError, 
                "Expected 'builtin' to be one of CMPLEXICAL, CMPDECIMAL, CMPINT32, CMPINT64");
            return NULL;
        }
        
        Py_BEGIN_ALLOW_THREADS
        success = tcbdbsetcmpfunc(self->db, *BTree_builtin_cmp[builtin], NULL);
        Py_END_ALLOW_THREADS
        
        if (!success)
        {
            raise_btree_error(self->db);
            return NULL;
        }
        Py_RETURN_NONE;
    }
    
    if (!PyCallable_Check(cmp))
    {
        return NULL;
    }
    
    if (!cmpop)
    {
        Py_INCREF(Py_None);
        cmpop = Py_None;
    }
    
    Py_INCREF(cmp);
    Py_XINCREF(cmpop);
    Py_XDECREF(self->cmp);
    Py_XDECREF(self->cmpop);
    
    self->cmp = cmp;
    self->cmpop = cmpop;
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbsetcmpfunc(self->db, (BDBCMP) BTree_cmpfunc, self);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        Py_DECREF(self->cmp);
        Py_XDECREF(self->cmpop);
        self->cmp = self->cmpop = NULL;
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_tune(BTree *self, PyObject *args, PyObject *kwargs)
{
    bool success = 0;
    int lmemb, nmemb;
    lmemb = nmemb = 0;
    PY_LONG_LONG bnum = 0;
    short apow, bpow;
    apow = bpow = -1;
    unsigned char opts = 0;
    
    static char *kwlist[] = {"lmemb", "nmemb", "bnum", "apow", "bpow", "opts", NULL };
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iiLhhB:tune", kwlist, 
        &lmemb, &nmemb, &bnum, &apow, &bpow, &opts))
    {
        return NULL;
    }
    
    if (apow < SCHAR_MIN || apow > SCHAR_MAX ||
        bpow < SCHAR_MIN || bpow > SCHAR_MAX)
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbtune(self->db, lmemb, nmemb, bnum, apow, bpow, opts);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_setcache(BTree *self, PyObject *args, PyObject *kwargs)
{
    bool success = 0;
    int lcnum, ncnum;
    lcnum = ncnum = 0;
    
    static char *kwlist[] = {"lcnum", "ncnum", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ii", kwlist, &lcnum, &ncnum))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbsetcache(self->db, lcnum, ncnum);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_setxmsize(BTree *self, PyObject *args)
{
    bool success = 0;
    PY_LONG_LONG xmsiz = 0;
    
    if (!PyArg_ParseTuple(args, "L", &xmsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbsetxmsiz(self->db, xmsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_open(BTree *self, PyObject *args, PyObject *kwargs)
{
    int omode = BDBOWRITER | BDBOCREAT;
    char *path = NULL;
    static char *kwlist[] = { "path", "omode", NULL };
    
    if (PyArg_ParseTupleAndKeywords(args, kwargs, "s|i:open", kwlist, &path, &omode))
    {
        bool success = 0;
        Py_BEGIN_ALLOW_THREADS
        success = tcbdbopen(self->db, path, omode);
        Py_END_ALLOW_THREADS
        if (success)
        {
            Py_RETURN_NONE;
        }
        raise_btree_error(self->db);
    }
    return NULL;
}


static PyObject *
BTree_close(BTree *self)
{
    bool success = 0;
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbclose(self->db);
    Py_END_ALLOW_THREADS
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_put(BTree *self, PyObject *args)
{
    bool success;
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#s#:put", &kbuf, &ksiz, &vbuf, &vsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbput(self->db, kbuf, ksiz, vbuf, vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_putkeep(BTree *self, PyObject *args)
{
    bool success;
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#s#:putkeep", &kbuf, &ksiz, &vbuf, &vsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbputkeep(self->db, kbuf, ksiz, vbuf, vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_putcat(BTree *self, PyObject *args)
{
    bool success;
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#s#:putcat", &kbuf, &ksiz, &vbuf, &vsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbputcat(self->db, kbuf, ksiz, vbuf, vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_putdup(BTree *self, PyObject *args)
{
    bool success;
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#s#:putdup", &kbuf, &ksiz, &vbuf, &vsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbputdup(self->db, kbuf, ksiz, vbuf, vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_out(BTree *self, PyObject *args)
{
    bool success;
    char *kbuf;
    int ksiz;
    
    if (!PyArg_ParseTuple(args, "s#:out", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbout(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_outdup(BTree *self, PyObject *args)
{
    bool success;
    char *kbuf;
    int ksiz;
    
    if (!PyArg_ParseTuple(args, "s#:outdup", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbout3(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_get(BTree *self, PyObject *args, PyObject *kwargs)
{
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    PyObject *default_value = NULL;
    PyObject *value = NULL;
    
    static char *kwlist[] = {"key", "default", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#|O:outdup", kwlist, &kbuf, &ksiz, &default_value))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    vbuf = tcbdbget(self->db, kbuf, ksiz, &vsiz);
    Py_END_ALLOW_THREADS
    
    if (!vbuf)
    {
        if (default_value)
        {
            return default_value;
        }
        Py_RETURN_NONE;
    }
    
    value = PyString_FromStringAndSize(vbuf, vsiz);
    free(vbuf);
    
    if (!value)
    {
        return NULL;
    }
    
    return value;
}


static PyObject *
BTree_getdup(BTree *self, PyObject *args)
{
    char *kbuf;
    int ksiz, i, n;
    PyObject *pylist;
    TCLIST *list;
    
    if (!PyArg_ParseTuple(args, "s#:getdup", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    list = tcbdbget4(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    if (!list)
    {
        raise_btree_error(self->db);
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
BTree_vnum(BTree *self, PyObject *args)
{
    char *kbuf;
    int ksiz, vnum;
    
    if (!PyArg_ParseTuple(args, "s#:vnum", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    vnum = tcbdbvnum(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    return Py_BuildValue("i", vnum);
}


static PyObject *
BTree_vsiz(BTree *self, PyObject *args)
{
    char *kbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#:vsiz", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    vsiz = tcbdbvsiz(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    return Py_BuildValue("i", vsiz);
}


static PyObject *
BTree_range(BTree *self, PyObject *args, PyObject *kwargs)
{
    char *bkbuf, *ekbuf;
    int bksiz, eksiz, binc, einc, i, n;
    int max = -1;
    PyObject *pylist;
    TCLIST *list;
    
    static char *kwlist[] = {"bk", "binc", "ek", "einc", "max", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#is#i|i:range", kwlist,
        &bkbuf, &bksiz, &binc, &ekbuf, &eksiz, &einc, &max))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    list = tcbdbrange(self->db, bkbuf, bksiz, binc, ekbuf, eksiz, einc, max);
    Py_END_ALLOW_THREADS
    
    if (!list)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memort for TCLIST object");
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
BTree_fwmkeys(BTree *self, PyObject *args, PyObject *kwargs)
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
    list = tcbdbfwmkeys(self->db, pbuf, psiz, max);
    Py_END_ALLOW_THREADS
    
    if (!list)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memort for TCLIST object");
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
BTree_addint(BTree *self, PyObject *args)
{
    char *kbuf;
    int ksiz, num, result;
    
    if (!PyArg_ParseTuple(args, "s#i:addint", &kbuf, &ksiz, &num))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    result = tcbdbaddint(self->db, kbuf, ksiz, num);
    Py_END_ALLOW_THREADS
    
    return PyInt_FromLong((long) result);
}


static PyObject *
BTree_adddouble(BTree *self, PyObject *args)
{
    char *kbuf;
    int ksiz;
    double num, result;
    
    if (!PyArg_ParseTuple(args, "s#d:adddouble", &kbuf, &ksiz, &num))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    result = tcbdbadddouble(self->db, kbuf, ksiz, num);
    Py_END_ALLOW_THREADS
    
    return PyFloat_FromDouble(result);
}


static PyObject *
BTree_sync(BTree *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbsync(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
BTree_optimize(BTree *self, PyObject *args, PyObject *kwargs)
{
    bool success = 0;
    int lmemb, nmemb;
    lmemb = nmemb = 0;
    PY_LONG_LONG bnum = 0;
    short apow, bpow;
    apow = bpow = -1;
    unsigned char opts = UCHAR_MAX;
    
    static char *kwlist[] = {"lmemb", "nmemb", "bnum", "apow", "bpow", "opts", NULL };
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iiLhhB:tune", kwlist, 
        &lmemb, &nmemb, &bnum, &apow, &bpow, &opts))
    {
        return NULL;
    }
    
    if (apow < SCHAR_MIN || apow > SCHAR_MAX ||
        bpow < SCHAR_MIN || bpow > SCHAR_MAX)
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdboptimize(self->db, lmemb, nmemb, bnum, apow, bpow, opts);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
BTree_vanish(BTree *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbvanish(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
BTree_copy(BTree *self, PyObject *args)
{
    bool success;
    char *path;
    
    if (!PyArg_ParseTuple(args, "s", &path))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbcopy(self->db, path);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
BTree_tranbegin(BTree *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbtranbegin(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
BTree_trancommit(BTree *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbtrancommit(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
BTree_tranabort(BTree *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbtranabort(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
BTree_path(BTree *self)
{
    const char *path;
    
    Py_BEGIN_ALLOW_THREADS
    path = tcbdbpath(self->db);
    Py_END_ALLOW_THREADS
    
    if (!path)
    {
        Py_RETURN_NONE;
    }
    
    return PyString_FromString(path);
}


static PyObject *
BTree_rnum(BTree *self)
{
    uint64_t rnum;
    
    Py_BEGIN_ALLOW_THREADS
    rnum = tcbdbrnum(self->db);
    Py_END_ALLOW_THREADS
    
    return PyLong_FromLongLong(rnum);
}


static PyObject *
BTree_fsiz(BTree *self)
{
    uint64_t fsiz;
    
    Py_BEGIN_ALLOW_THREADS
    fsiz = tcbdbfsiz(self->db);
    Py_END_ALLOW_THREADS
    
    return PyLong_FromLongLong(fsiz);
}


static PyObject *
BTree_cursor(BTree *self)
{
    PyObject *pycur;
    PyObject *args;
    
    args = Py_BuildValue("(O)", self);
    pycur = BTreeCursor_new(&BTreeCursorType, args, NULL);
    Py_DECREF(args);
    
    if (!pycur)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    
    return pycur;
}


static Py_ssize_t
BTree_length(BTree *self)
{
    uint64_t rnum;
    
    Py_BEGIN_ALLOW_THREADS
    rnum = tcbdbrnum(self->db);
    Py_END_ALLOW_THREADS
    
    return (Py_ssize_t) rnum;
}


static PyObject *
BTree_subscript(BTree *self, PyObject *key)
{
    char *kbuf, *vbuf;
    Py_ssize_t ksiz;
    int vsiz;
    PyObject *value;
    
    if (!PyString_Check(key))
    {
        PyErr_SetString(PyExc_ValueError, "Expected key to be a string.");
        return NULL;
    }
    
    PyString_AsStringAndSize(key, &kbuf, &ksiz);
    
    if (!kbuf)
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    vbuf = tcbdbget(self->db, kbuf, (int) ksiz, &vsiz);
    Py_END_ALLOW_THREADS
    
    if (!vbuf)
    {
        raise_btree_error(self->db);
        return NULL;
    }
    
    value = PyString_FromStringAndSize(vbuf, vsiz);
    free(vbuf);
    
    if (!value)
    {
        return NULL;
    }
    
    return value;
}


static int
BTree_ass_subscript(BTree *self, PyObject *key, PyObject *value)
{
    bool success;
    char *kbuf, *vbuf;
    Py_ssize_t ksiz, vsiz;
    
    if (!PyString_Check(key))
    {
        PyErr_SetString(PyExc_ValueError, "Expected key to be a string.");
        return -1;
    }
    
    if (!PyString_Check(value))
    {
        PyErr_SetString(PyExc_ValueError, "Expected value to be a string.");
        return -1;
    }
    
    PyString_AsStringAndSize(key, &kbuf, &ksiz);
    if (!kbuf)
    {
        return -1;
    }
    
    PyString_AsStringAndSize(value, &vbuf, &vsiz);
    if (!vbuf)
    {
        return -1;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tcbdbput(self->db, kbuf, (int) ksiz, vbuf, (int) vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_btree_error(self->db);
        return -1;
    }
    
    return 0;
}


static PyMappingMethods BTree_as_mapping = 
{
    (lenfunc) BTree_length,
    (binaryfunc) BTree_subscript,
    (objobjargproc) BTree_ass_subscript
};


static int
BTree_contains(BTree *self, PyObject *value)
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
    vsiz = tcbdbvsiz(self->db, kbuf, (int) ksiz);
    Py_END_ALLOW_THREADS
    
    return vsiz != -1;
}


static PySequenceMethods BTree_as_sequence = 
{
    0,                             /* sq_length */
    0,                             /* sq_concat */
    0,                             /* sq_repeat */
    0,                             /* sq_item */
    0,                             /* sq_slice */
    0,                             /* sq_ass_item */
    0,                             /* sq_ass_slice */
    (objobjproc) BTree_contains,   /* sq_contains */
    0,                             /* sq_inplace_concat */
    0                              /* sq_inplace_repeat */
};


static PyMethodDef BTree_methods[] = 
{
    {
        "setmutex", (PyCFunction) BTree_setmutex,
        METH_NOARGS,
        "Set the mutual exclusion control of the database for threading."
    },
    
    {
        "setcmpfunc", (PyCFunction) BTree_setcmpfunc,
        METH_VARARGS | METH_KEYWORDS,
        "Set the key comparison function."
    },
    
    {
        "tune", (PyCFunction) BTree_tune,
        METH_VARARGS | METH_KEYWORDS,
        "Set tuning parameters."
    },
    
    {
        "setcache", (PyCFunction) BTree_setcache,
        METH_VARARGS | METH_KEYWORDS,
        "Set cache parameters."
    },
    
    {
        "setxmsize", (PyCFunction) BTree_setxmsize,
        METH_VARARGS | METH_KEYWORDS,
        "Set size of extra mapped memory."
    },
    
    {
        "open", (PyCFunction) BTree_open, 
        METH_VARARGS | METH_KEYWORDS,
        "Open the database at the given path in the given mode"
    },
    
    {
        "close", (PyCFunction) BTree_close,
        METH_NOARGS,
        "Close the database"
    },
    
    {
        "put", (PyCFunction) BTree_put,
        METH_VARARGS,
        "Store a record. Overwrite existing record."
    },
    
    {
        "putkeep", (PyCFunction) BTree_putkeep,
        METH_VARARGS,
        "Store a record. Don't overwrite an existing record."
    },
    
    {
        "putcat", (PyCFunction) BTree_putcat,
        METH_VARARGS,
        "Concatenate value on the end of a record. Creates the record if it doesn't exist."
    },
    
    {
        "putdup", (PyCFunction) BTree_putdup,
        METH_VARARGS,
        "Store a record. If a corresponding record exists, insert a new one after it."
    },
    
    {
        "out", (PyCFunction) BTree_out,
        METH_VARARGS,
        "Remove a record. If there are duplicates only the first is removed."
    },
    
    {
        "outdup", (PyCFunction) BTree_outdup,
        METH_VARARGS,
        "Remove a record. If there are duplicates all of them are removed."
    },
    
    {
        "get", (PyCFunction) BTree_get,
        METH_VARARGS | METH_KEYWORDS,
        "Retrieve a record. If none is found None or the supplied default value is returned."
    },
    
    {
        "getdup", (PyCFunction) BTree_getdup,
        METH_VARARGS,
        "Retrieve a list of records with the same key."
    },
    
    {
        "vnum", (PyCFunction) BTree_vnum,
        METH_VARARGS,
        "Get the number of records for a key."
    },
    
    {
        "vsiz", (PyCFunction) BTree_vsiz,
        METH_VARARGS,
        "Get the size of the of the record for key. If duplicates are found, the first record is used."
    },
    
    {
        "range", (PyCFunction) BTree_range,
        METH_VARARGS | METH_KEYWORDS,
        "Get a list of keys in the given range."
    },
    
    {
        "fwmkeys", (PyCFunction) BTree_fwmkeys,
        METH_VARARGS | METH_KEYWORDS,
        "Get a list of of keys that match the given prefix."
    },
    
    {
        "addint", (PyCFunction) BTree_addint,
        METH_VARARGS,
        "Add an integer to the selected record."
    },
    
    {
        "adddouble", (PyCFunction) BTree_adddouble,
        METH_VARARGS,
        "Add a double to the selected record."
    },
    
    {
        "sync", (PyCFunction) BTree_sync,
        METH_NOARGS,
        "Sync data with the disk device."
    },
    
    {
        "optimize", (PyCFunction) BTree_optimize,
        METH_VARARGS,
        "Optimize a fragmented database."
    },
    
    {
        "vanish", (PyCFunction) BTree_vanish,
        METH_NOARGS,
        "Remove all records from the database."
    },
    
    {
        "copy", (PyCFunction) BTree_copy,
        METH_VARARGS,
        "Copy the database to a new file."
    },
    
    {
        "tranbegin", (PyCFunction) BTree_tranbegin,
        METH_NOARGS,
        "Start transaction."
    },
    
    {
        "trancommit", (PyCFunction) BTree_trancommit,
        METH_NOARGS,
        "Commit transaction."
    },
    
    {
        "tranabort", (PyCFunction) BTree_tranabort,
        METH_NOARGS,
        "Abort transaction."
    },
    
    {
        "path", (PyCFunction) BTree_path,
        METH_NOARGS,
        "Get the path of the database file or None if unconnected."
    },
    
    {
        "rnum", (PyCFunction) BTree_rnum,
        METH_NOARGS,
        "Get the number of records in the database."
    },
    
    {
        "fsiz", (PyCFunction) BTree_fsiz,
        METH_NOARGS,
        "Get the size of the database in bytes."
    },
    
    {
        "cursor", (PyCFunction) BTree_cursor,
        METH_NOARGS,
        "Get a cursor for the database."
    },
    
    { NULL }
};


static PyTypeObject BTreeType = {
  PyObject_HEAD_INIT(NULL)
  0,                                           /* ob_size */
  "tokyocabinet.btree.BTree",                  /* tp_name */
  sizeof(BTree),                               /* tp_basicsize */
  0,                                           /* tp_itemsize */
  (destructor)BTree_dealloc,                   /* tp_dealloc */
  0,                                           /* tp_print */
  0,                                           /* tp_getattr */
  0,                                           /* tp_setattr */
  0,                                           /* tp_compare */
  0,                                           /* tp_repr */
  0,                                           /* tp_as_number */
  &BTree_as_sequence,                          /* tp_as_sequence */
  &BTree_as_mapping,                           /* tp_as_mapping */
  BTree_Hash,                                  /* tp_hash  */
  0,                                           /* tp_call */
  0,                                           /* tp_str */
  0,                                           /* tp_getattro */
  0,                                           /* tp_setattro */
  0,                                           /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,    /* tp_flags */
  "BTree database",                            /* tp_doc */
  0,                                           /* tp_traverse */
  0,                                           /* tp_clear */
  0,                                           /* tp_richcompare */
  0,                                           /* tp_weaklistoffset */
  0,                                           /* tp_iter */
  0,                                           /* tp_iternext */
  BTree_methods,                               /* tp_methods */
  0,                                           /* tp_members */
  0,                                           /* tp_getset */
  0,                                           /* tp_base */
  0,                                           /* tp_dict */
  0,                                           /* tp_descr_get */
  0,                                           /* tp_descr_set */
  0,                                           /* tp_dictoffset */
  0,                                           /* tp_init */
  0,                                           /* tp_alloc */
  BTree_new,                                   /* tp_new */
};


#define ADD_INT_CONSTANT(module, CONSTANT) PyModule_AddIntConstant(module, #CONSTANT, CONSTANT)

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initbtree(void)
{
    PyObject *m;
    
    m = Py_InitModule3(
            "tokyocabinet.btree", NULL, 
            "Tokyo cabinet BTree database wrapper"
    );
    
    if (!m)
    {
        return;
    }
    
    BTreeError = PyErr_NewException("tokyocabinet.btree.error", NULL, NULL);
    Py_INCREF(BTreeError);
    PyModule_AddObject(m, "error", BTreeError);
    
    if (PyType_Ready(&BTreeType) < 0)
    {
        return;
    }
    
    if (PyType_Ready(&BTreeCursorType) < 0)
    {
        return;
    }
    
    
    Py_INCREF(&BTreeType);
    PyModule_AddObject(m, "BTree", (PyObject *) &BTreeType);
    
    Py_INCREF(&BTreeCursorType);
    PyModule_AddObject(m, "BTreeCursor", (PyObject *) &BTreeCursorType);
    
    ADD_INT_CONSTANT(m, BDBOREADER);
    ADD_INT_CONSTANT(m, BDBOWRITER);
    ADD_INT_CONSTANT(m, BDBOCREAT);
    ADD_INT_CONSTANT(m, BDBOTRUNC);
    ADD_INT_CONSTANT(m, BDBOTSYNC);
    ADD_INT_CONSTANT(m, BDBONOLCK);
    ADD_INT_CONSTANT(m, BDBOLCKNB);
    
    ADD_INT_CONSTANT(m, BDBTLARGE);
    ADD_INT_CONSTANT(m, BDBTDEFLATE);
    ADD_INT_CONSTANT(m, BDBTBZIP);
    ADD_INT_CONSTANT(m, BDBTTCBS);
    
    PyModule_AddIntConstant(m, "CMPLEXICAL", 1);
    PyModule_AddIntConstant(m, "CMPDECIMAL", 2);
    PyModule_AddIntConstant(m, "CMPINT32", 3);
    PyModule_AddIntConstant(m, "CMPINT64", 4);
    
    ADD_INT_CONSTANT(m, BDBCPCURRENT);
    ADD_INT_CONSTANT(m, BDBCPBEFORE);
    ADD_INT_CONSTANT(m, BDBCPAFTER);
}
