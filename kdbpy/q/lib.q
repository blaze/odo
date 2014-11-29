\d .kdbpy

qp: .Q.qp

is_table: .Q.qt
is_keyed_table: {[x] is_table[x] & (typename[x] = `dict)}

typenums: `short$(0 1 2 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 98 99)
longnames: (`list`boolean`guid`byte`short`int`long`real`float`char`symbol,
            `timestamp`month`date`datetime`timespan`minute`second`time`table,
            `dict)
types: typenums!longnames

typename: {[x] types[abs[type[x]]]}

is_long: {[x] typename[x] = `long}

is_partitioned: {[x]
    p: qp[x];
    $[is_long[p]; 0b; p]}

is_splayed: {[x]
    p: qp[x];
    $[is_long[p]; 0b; not p]}

index_into: {[x; indices]
    $[is_partitioned[x];
        .Q.ind[x; indices];
        $[is_keyed_table[x];
            x[key[x][indices]]; // have to slice with the index here
            x[indices]]]}


// q repeats if the N of take is larger than the number of rows, so we
// need to get the min of the number of rows and the requested N from the
// Head expression
gen_indices: {[x; start; stop]
    nrows: count x;
    start: $[start < 0; start + nrows; start];

    // <= here otherwise we get things like s[-1:0] == s[nrows:0]
    // when it really should be s[-1:0] == s[nrows - 1:nrows]
    stop: $[stop <= 0; stop + nrows; stop];
    stop: stop & nrows;
    indices: start + (til (stop - start));
    indices}

slice1: {[x; index]
    result: index_into[x; gen_indices[x; index; index + 1][0]];
    $[typename[result] = `dict; enlist result; result]}

slice: {[x; start; stop]
    index_into[x; gen_indices[x; start; stop]]}

get_partitioned_field: {[table; name]
    namelist: enlist[name];
    (?; (?; table; (); 0b; namelist!namelist); (); (); namelist)}

get_field: {[table; name]
    if [typename[table] <> `symbol;
        '`$"TypeError: first argument must be a symbol"];
    if [typename[name] <> `symbol;
        '`$"TypeError: second argument must be a symbol"];
    t: eval table;
    if [not name in cols t;
        '`$"ValueError: passed in field not in table columns"];
    $[is_partitioned[t];
        get_partitioned_field[table; name];
        $[is_splayed[t];
            (table; name);
            "" sv (string table; enlist["."]; string name)]]}


nunique: {[x] count distinct x}

\d .
