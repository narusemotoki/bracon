BraCon
######

This library is Branch and Confluence. You can have consecutive another process.

.. code-block::

   timeline >-------------------------------->

   main process o---o---o---o---o---o---o---o---o
                     \       \           \
   sub process        o-------o-----------o

Requirements
============

* Python3

Quick Example
=============

.. code-block:: python

   import bracon

   b = bracon.Bracon()
   b.add(lambda x: "Hello")
   b.add(lambda x: x * 2)
   b.add(lambda x: print("print from sub process", x))
   print("print from main process")

When you run this script, you will get:

.. code-block::

   print from main process
   print from sub process HelloHello

But if you copy and paste on your python shell one by one, you will get:

.. code-block::

   print from sub process HelloHello
   print from main process

Sometime main is called fist, sometime sub is called first. That's why BraCon runs on another process.
