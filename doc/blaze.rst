``Q`` and ``blaze``: a match made in heaven
===========================================

One of the goals of ``kdbpy`` is to hide the Q language from the user. To that
end, here are a few things that need to be abstracted to make this successful.

- Partitioned, splayed and standard/in-memory tables need to look the same from
  within Python. That is, one should be able to use blaze expressions without
  knowledge of the type of table being computed over.


Partitioned Tables
------------------
- Expressions *must* start with a ``select`` statement. Blaze needs to enforce
  this.

- Expressions should contain the virtual column as the first conditional in the
  ``where`` clause of the ``select`` statement. This isn't enforced by ``q``
  but is useful for getting high performance out of the selection. Blaze can
  help here by: 1) reordering the ``where`` clause such that the virtual column
  is first. For example, consider daily OHLC of the AAPL symbol where our table
  is partitioned by year ::
    select o: first price, h: max price, l: min price, c: last price by
        date.day from t where sym = `AAPL, date.year within 2009 2012

  The ``where`` clause would be reordered to be ::
        ... where date.year within 2009 2012, sym = `AAPL
