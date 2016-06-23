"""This library is Branch and Confluence.

You can have consecutive another process.
"""
import multiprocessing
import multiprocessing.managers
import queue
import threading
import types


__all__ = ['Bracon', 'initial']
initial = type('Initial', (), {})()


class Bracon:
    """This Bracon will call added function one by one.

    Function queue is first in first out.
    """

    def __init__(self) -> None:
        self._queue = queue.Queue()
        self._t = None
        self._last = initial

    def add(self, func: types.FunctionType) -> None:
        """Add a function into a queue.

        Passed function must take one argument. The function will be called
        with returning value of added right before function. If this function
        is the first, it will be called with initial object.

        If you want to pass any parameter to the function, you can do something
        like this:

        .. code-block:: python

           b = bracon.Bracon()
           b.add(lambda x: your_function(x, params))

        :params func: is a function, will be added into a queue.
        """
        self._queue.put(func)
        if not self.is_alive():
            self._t = threading.Thread(target=self._worker)
            self._t.start()

    def is_alive(self) -> bool:
        """Return whether any added function is running.
        """
        return self._t is not None and self._t.is_alive()

    def join(self) -> None:
        """Wait until the queue gets empty and all added function process finished.
        """
        if self.is_alive():
            self._t.join()

    def _loop(self, shared: multiprocessing.managers.NamespaceProxy) -> None:
        while True:
            try:
                func = self._queue.get_nowait()
            except queue.Empty:
                break
            process = multiprocessing.Process(target=self._wapper(func, shared))
            process.start()
            process.join()
            self._last = shared.returning

    def _worker(self) -> None:
        with multiprocessing.Manager() as manager:
            self._loop(manager.Namespace())

    def _wapper(self, func: types.FunctionType,
                shared:  multiprocessing.managers.NamespaceProxy) -> None:
        shared.returning = func(self._last)

    @property
    def last(self):
        return self._last

    @last.setter
    def last(self, last) -> None:
        raise NotImplementedError
