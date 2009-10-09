#include <Python.h>
#include <tchdb.h>
#include <tcutil.h>
#include <limits.h>


static PyObject *HashError;


static void
raise_hash_error(TCHDB *db)
{
    int code = tchdbecode(db);
    const char *msg = tchdberrmsg(code);
    
    if (code == TCENOREC)
    {
        PyErr_SetString(PyExc_KeyError, msg);
    }
    else
    {
        PyErr_SetString(HashError, msg);
    }
}


static PyTypeObject HashType;


typedef struct
{
    PyObject_HEAD
    TCHDB *db;
} Hash;


static long
Hash_Hash(PyObject *self)
{
    PyErr_SetString(PyExc_TypeError, "Hash objects are not hashable.");
    return -1;
}


static void
Hash_dealloc(Hash *self)
{
    if (self->db)
    {
        Py_BEGIN_ALLOW_THREADS
        tchdbdel(self->db);
        Py_END_ALLOW_THREADS
    }
    self->ob_type->tp_free(self);
}


static PyObject *
Hash_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    Hash *self;
    
    self = (Hash *) type->tp_alloc(type, 0);
    if (!self)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate Hash instance.");
        return NULL;
    }
    
    self->db = tchdbnew();
    if (!self->db)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate TCHDB instance.");
        return NULL;
    }
    
    int omode = HDBOWRITER | HDBOCREAT;
    char *path = NULL;
    static char *kwlist[] = { "path", "omode", NULL };
    
    if (PyArg_ParseTupleAndKeywords(args, kwargs, "|si", kwlist, &path, &omode))
    {
        if (path)
        {
            bool success = 0;
            Py_BEGIN_ALLOW_THREADS
            success = tchdbopen(self->db, path, omode);
            Py_END_ALLOW_THREADS
            if (success)
            {
                return (PyObject *) self;
            }
            raise_hash_error(self->db);
        }
        else
        {
            return (PyObject *) self;
        }
    }
    
    Hash_dealloc(self);
    return NULL;
}


static PyObject *
Hash_setmutex(Hash *self)
{
    bool success = 0;
    Py_BEGIN_ALLOW_THREADS
    success = tchdbsetmutex(self->db);
    Py_END_ALLOW_THREADS
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_tune(Hash *self, PyObject *args, PyObject *kwargs)
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
    success = tchdbtune(self->db, bnum, apow, fpow, opts);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_setcache(Hash *self, PyObject *args, PyObject *kwargs)
{
    bool success = 0;
    int rcnum;
    rcnum = 0;
    
    static char *kwlist[] = {"rcnum", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i", kwlist, &rcnum))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbsetcache(self->db, rcnum);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_setxmsize(Hash *self, PyObject *args)
{
    bool success = 0;
    PY_LONG_LONG xmsiz = 0;
    
    if (!PyArg_ParseTuple(args, "L", &xmsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbsetxmsiz(self->db, xmsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_open(Hash *self, PyObject *args, PyObject *kwargs)
{
    int omode = HDBOWRITER | HDBOCREAT;
    char *path = NULL;
    static char *kwlist[] = { "path", "omode", NULL };
    
    if (PyArg_ParseTupleAndKeywords(args, kwargs, "s|i:open", kwlist, &path, &omode))
    {
        bool success = 0;
        Py_BEGIN_ALLOW_THREADS
        success = tchdbopen(self->db, path, omode);
        Py_END_ALLOW_THREADS
        if (success)
        {
            Py_RETURN_NONE;
        }
        raise_hash_error(self->db);
    }
    return NULL;
}


static PyObject *
Hash_close(Hash *self)
{
    bool success = 0;
    Py_BEGIN_ALLOW_THREADS
    success = tchdbclose(self->db);
    Py_END_ALLOW_THREADS
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_put(Hash *self, PyObject *args)
{
    bool success;
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#s#:put", &kbuf, &ksiz, &vbuf, &vsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbput(self->db, kbuf, ksiz, vbuf, vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_putkeep(Hash *self, PyObject *args)
{
    bool success;
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#s#:putkeep", &kbuf, &ksiz, &vbuf, &vsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbputkeep(self->db, kbuf, ksiz, vbuf, vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_putcat(Hash *self, PyObject *args)
{
    bool success;
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#s#:putcat", &kbuf, &ksiz, &vbuf, &vsiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbputcat(self->db, kbuf, ksiz, vbuf, vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_out(Hash *self, PyObject *args)
{
    bool success;
    char *kbuf;
    int ksiz;
    
    if (!PyArg_ParseTuple(args, "s#:out", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbout(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_get(Hash *self, PyObject *args, PyObject *kwargs)
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
    vbuf = tchdbget(self->db, kbuf, ksiz, &vsiz);
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
Hash_vsiz(Hash *self, PyObject *args)
{
    char *kbuf;
    int ksiz, vsiz;
    
    if (!PyArg_ParseTuple(args, "s#:vsiz", &kbuf, &ksiz))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    vsiz = tchdbvsiz(self->db, kbuf, ksiz);
    Py_END_ALLOW_THREADS
    
    return Py_BuildValue("i", vsiz);
}


static PyObject *
Hash_fwmkeys(Hash *self, PyObject *args, PyObject *kwargs)
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
    list = tchdbfwmkeys(self->db, pbuf, psiz, max);
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
Hash_addint(Hash *self, PyObject *args)
{
    char *kbuf;
    int ksiz, num, result;
    
    if (!PyArg_ParseTuple(args, "s#i:addint", &kbuf, &ksiz, &num))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    result = tchdbaddint(self->db, kbuf, ksiz, num);
    Py_END_ALLOW_THREADS
    
    return PyInt_FromLong((long) result);
}


static PyObject *
Hash_adddouble(Hash *self, PyObject *args)
{
    char *kbuf;
    int ksiz;
    double num, result;
    
    if (!PyArg_ParseTuple(args, "s#d:adddouble", &kbuf, &ksiz, &num))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    result = tchdbadddouble(self->db, kbuf, ksiz, num);
    Py_END_ALLOW_THREADS
    
    return PyFloat_FromDouble(result);
}


static PyObject *
Hash_sync(Hash *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbsync(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Hash_optimize(Hash *self, PyObject *args, PyObject *kwargs)
{
    bool success = 0;
    PY_LONG_LONG bnum = 0;
    short apow, fpow;
    apow = fpow = -1;
    unsigned char opts = UCHAR_MAX;
    
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
    success = tchdboptimize(self->db, bnum, apow, fpow, opts);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Hash_vanish(Hash *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbvanish(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Hash_copy(Hash *self, PyObject *args)
{
    bool success;
    char *path;
    
    if (!PyArg_ParseTuple(args, "s", &path))
    {
        return NULL;
    }
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbcopy(self->db, path);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Hash_tranbegin(Hash *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbtranbegin(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Hash_trancommit(Hash *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbtrancommit(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Hash_tranabort(Hash *self)
{
    bool success;
    
    Py_BEGIN_ALLOW_THREADS
    success = tchdbtranabort(self->db);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return NULL;
    }
    
    Py_RETURN_NONE;
}


static PyObject *
Hash_path(Hash *self)
{
    const char *path;
    
    Py_BEGIN_ALLOW_THREADS
    path = tchdbpath(self->db);
    Py_END_ALLOW_THREADS
    
    if (!path)
    {
        Py_RETURN_NONE;
    }
    
    return PyString_FromString(path);
}


static PyObject *
Hash_rnum(Hash *self)
{
    uint64_t rnum;
    
    Py_BEGIN_ALLOW_THREADS
    rnum = tchdbrnum(self->db);
    Py_END_ALLOW_THREADS
    
    return PyLong_FromLongLong(rnum);
}


static PyObject *
Hash_fsiz(Hash *self)
{
    uint64_t fsiz;
    
    Py_BEGIN_ALLOW_THREADS
    fsiz = tchdbfsiz(self->db);
    Py_END_ALLOW_THREADS
    
    return PyLong_FromLongLong(fsiz);
}


static int
Hash_length(Hash *self)
{
    uint64_t rnum;
    
    Py_BEGIN_ALLOW_THREADS
    rnum = tchdbrnum(self->db);
    Py_END_ALLOW_THREADS
    
    return (int) rnum;
}


static PyObject *
Hash_subscript(Hash *self, PyObject *key)
{
    char *kbuf, *vbuf;
    int ksiz;
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
    vbuf = tchdbget(self->db, kbuf, (int) ksiz, &vsiz);
    Py_END_ALLOW_THREADS
    
    if (!vbuf)
    {
        raise_hash_error(self->db);
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
Hash_ass_subscript(Hash *self, PyObject *key, PyObject *value)
{
    bool success;
    char *kbuf, *vbuf;
    int ksiz, vsiz;
    
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
    success = tchdbput(self->db, kbuf, (int) ksiz, vbuf, (int) vsiz);
    Py_END_ALLOW_THREADS
    
    if (!success)
    {
        raise_hash_error(self->db);
        return -1;
    }
    
    return 0;
}


static PyMappingMethods Hash_as_mapping = 
{
    (lenfunc) Hash_length,
    (binaryfunc) Hash_subscript,
    (objobjargproc) Hash_ass_subscript
};


static int
Hash_contains(Hash *self, PyObject *value)
{
    char *kbuf;
    int ksiz;
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
    vsiz = tchdbvsiz(self->db, kbuf, (int) ksiz);
    Py_END_ALLOW_THREADS
    
    return vsiz != -1;
}


static PySequenceMethods Hash_as_sequence = 
{
    0,                             /* sq_length */
    0,                             /* sq_concat */
    0,                             /* sq_repeat */
    0,                             /* sq_item */
    0,                             /* sq_slice */
    0,                             /* sq_ass_item */
    0,                             /* sq_ass_slice */
    (objobjproc) Hash_contains,   /* sq_contains */
    0,                             /* sq_inplace_concat */
    0                              /* sq_inplace_repeat */
};


static PyMethodDef Hash_methods[] = 
{
    {
        "setmutex", (PyCFunction) Hash_setmutex,
        METH_NOARGS,
        "Set the mutual exclusion control of the database for threading."
    },
    
    {
        "tune", (PyCFunction) Hash_tune,
        METH_VARARGS | METH_KEYWORDS,
        "Set tuning parameters."
    },
    
    {
        "setcache", (PyCFunction) Hash_setcache,
        METH_VARARGS | METH_KEYWORDS,
        "Set cache parameters."
    },
    
    {
        "setxmsize", (PyCFunction) Hash_setxmsize,
        METH_VARARGS | METH_KEYWORDS,
        "Set size of extra mapped memory."
    },
    
    {
        "open", (PyCFunction) Hash_open, 
        METH_VARARGS | METH_KEYWORDS,
        "Open the database at the given path in the given mode"
    },
    
    {
        "close", (PyCFunction) Hash_close,
        METH_NOARGS,
        "Close the database"
    },
    
    {
        "put", (PyCFunction) Hash_put,
        METH_VARARGS,
        "Store a record. Overwrite existing record."
    },
    
    {
        "putkeep", (PyCFunction) Hash_putkeep,
        METH_VARARGS,
        "Store a record. Don't overwrite an existing record."
    },
    
    {
        "putcat", (PyCFunction) Hash_putcat,
        METH_VARARGS,
        "Concatenate value on the end of a record. Creates the record if it doesn't exist."
    },
    
    {
        "out", (PyCFunction) Hash_out,
        METH_VARARGS,
        "Remove a record. If there are duplicates only the first is removed."
    },
    
    {
        "get", (PyCFunction) Hash_get,
        METH_VARARGS | METH_KEYWORDS,
        "Retrieve a record. If none is found None or the supplied default value is returned."
    },
    
    {
        "vsiz", (PyCFunction) Hash_vsiz,
        METH_VARARGS,
        "Get the size of the of the record for key. If duplicates are found, the first record is used."
    },
    
    {
        "fwmkeys", (PyCFunction) Hash_fwmkeys,
        METH_VARARGS | METH_KEYWORDS,
        "Get a list of of keys that match the given prefix."
    },
    
    {
        "addint", (PyCFunction) Hash_addint,
        METH_VARARGS,
        "Add an integer to the selected record."
    },
    
    {
        "adddouble", (PyCFunction) Hash_adddouble,
        METH_VARARGS,
        "Add a double to the selected record."
    },
    
    {
        "sync", (PyCFunction) Hash_sync,
        METH_NOARGS,
        "Sync data with the disk device."
    },
    
    {
        "optimize", (PyCFunction) Hash_optimize,
        METH_VARARGS,
        "Optimize a fragmented database."
    },
    
    {
        "vanish", (PyCFunction) Hash_vanish,
        METH_NOARGS,
        "Remove all records from the database."
    },
    
    {
        "copy", (PyCFunction) Hash_copy,
        METH_VARARGS,
        "Copy the database to a new file."
    },
    
    {
        "tranbegin", (PyCFunction) Hash_tranbegin,
        METH_NOARGS,
        "Start transaction."
    },
    
    {
        "trancommit", (PyCFunction) Hash_trancommit,
        METH_NOARGS,
        "Commit transaction."
    },
    
    {
        "tranabort", (PyCFunction) Hash_tranabort,
        METH_NOARGS,
        "Abort transaction."
    },
    
    {
        "path", (PyCFunction) Hash_path,
        METH_NOARGS,
        "Get the path of the database file or None if unconnected."
    },
    
    {
        "rnum", (PyCFunction) Hash_rnum,
        METH_NOARGS,
        "Get the number of records in the database."
    },
    
    {
        "fsiz", (PyCFunction) Hash_fsiz,
        METH_NOARGS,
        "Get the size of the database in bytes."
    },
    
    { NULL }
};


static PyTypeObject HashType = {
  PyObject_HEAD_INIT(NULL)
  0,                                           /* ob_size */
  "tokyocabinet.hash.Hash",                  /* tp_name */
  sizeof(Hash),                               /* tp_basicsize */
  0,                                           /* tp_itemsize */
  (destructor)Hash_dealloc,                   /* tp_dealloc */
  0,                                           /* tp_print */
  0,                                           /* tp_getattr */
  0,                                           /* tp_setattr */
  0,                                           /* tp_compare */
  0,                                           /* tp_repr */
  0,                                           /* tp_as_number */
  &Hash_as_sequence,                          /* tp_as_sequence */
  &Hash_as_mapping,                           /* tp_as_mapping */
  Hash_Hash,                                  /* tp_hash  */
  0,                                           /* tp_call */
  0,                                           /* tp_str */
  0,                                           /* tp_getattro */
  0,                                           /* tp_setattro */
  0,                                           /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,    /* tp_flags */
  "Hash database",                            /* tp_doc */
  0,                                           /* tp_traverse */
  0,                                           /* tp_clear */
  0,                                           /* tp_richcompare */
  0,                                           /* tp_weaklistoffset */
  0,                                           /* tp_iter */
  0,                                           /* tp_iternext */
  Hash_methods,                               /* tp_methods */
  0,                                           /* tp_members */
  0,                                           /* tp_getset */
  0,                                           /* tp_base */
  0,                                           /* tp_dict */
  0,                                           /* tp_descr_get */
  0,                                           /* tp_descr_set */
  0,                                           /* tp_dictoffset */
  0,                                           /* tp_init */
  0,                                           /* tp_alloc */
  Hash_new,                                   /* tp_new */
};


#define ADD_INT_CONSTANT(module, CONSTANT) PyModule_AddIntConstant(module, #CONSTANT, CONSTANT)

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
inithash(void)
{
    PyObject *m;
    
    m = Py_InitModule3(
            "tokyocabinet.hash", NULL, 
            "Tokyo cabinet hash table wrapper"
    );
    
    if (!m)
    {
        return;
    }
    
    HashError = PyErr_NewException("tokyocabinet.hash.error", NULL, NULL);
    Py_INCREF(HashError);
    PyModule_AddObject(m, "error", HashError);
    
    if (PyType_Ready(&HashType) < 0)
    {
        return;
    }
    
    
    Py_INCREF(&HashType);
    PyModule_AddObject(m, "Hash", (PyObject *) &HashType);
    
    ADD_INT_CONSTANT(m, HDBOREADER);
    ADD_INT_CONSTANT(m, HDBOWRITER);
    ADD_INT_CONSTANT(m, HDBOCREAT);
    ADD_INT_CONSTANT(m, HDBOTRUNC);
    ADD_INT_CONSTANT(m, HDBOTSYNC);
    ADD_INT_CONSTANT(m, HDBONOLCK);
    ADD_INT_CONSTANT(m, HDBOLCKNB);
    
    ADD_INT_CONSTANT(m, HDBTLARGE);
    ADD_INT_CONSTANT(m, HDBTDEFLATE);
    ADD_INT_CONSTANT(m, HDBTBZIP);
    ADD_INT_CONSTANT(m, HDBTTCBS);
}
