CREATE OR REPLACE FUNCTION prepare_experiments() RETURNS void AS $$
DECLARE
BEGIN
    EXECUTE format('CREATE TABLE t_stats_rows (rows int, time float);');
    EXECUTE format('CREATE TABLE t_stats_col (cols int, time float);');
    EXECUTE format('CREATE TABLE t_random_cols (A float,B float,C float,D float,E float,F float,G float, H float,I float,J float,K float, L float, M float, N float, O float, P float, Q float, R float, S float, T float, U float, V float, W float, X float, Y float, Z float, AA float, BB float, CC float, DD float);');
    EXECUTE format('CREATE TABLE t_random_rows (A float,B float,C float,D float,E float)');
    EXECUTE format('INSERT INTO t_random_cols (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, BB, CC, DD) SELECT random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100, random()*100 from generate_series(1,10000000);');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_cols(repetitions int) RETURNS void AS $$
DECLARE
   _start_ts timestamptz;
   _end_ts   timestamptz;
    counter int;
BEGIN
    FOR counter in 1..(repetitions)
    LOOP

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift(A)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 1, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift2(A,B)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 2, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift3(A,B,C)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 3, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift4(A,B,C,D)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 4, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift5(A,B,C,D,E)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 5, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift5(A,B,C,D,E)*lift5(F,G,H,I,J)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 10, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift5(A,B,C,D,E)*lift5(F,G,H,I,J)*lift5(K,L,M,N,O)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 15, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift5(A,B,C,D,E)*lift5(F,G,H,I,J)*lift5(K,L,M,N,O)*lift5(P,Q,R,S,T)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 20, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift5(A,B,C,D,E)*lift5(F,G,H,I,J)*lift5(K,L,M,N,O)*lift5(P,Q,R,S,T)*lift5(U,V,W,X,Y)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 25, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift5(A,B,C,D,E)*lift5(F,G,H,I,J)*lift5(K,L,M,N,O)*lift5(P,Q,R,S,T)*lift5(U,V,W,X,Y)*lift5(Z,AA,BB,CC,DD)) FROM t_random_cols;');
    _end_ts := clock_timestamp();

    EXECUTE format('INSERT INTO t_stats_col (cols, time) VALUES (%s, %s)', 30, 1000 * (extract(epoch FROM _end_ts - _start_ts)));

    end LOOP;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test_rows(rows int[], repetitions int) RETURNS void AS $$
DECLARE
   _start_ts timestamptz;
   _end_ts   timestamptz;
    n_row int;
    n_row_prev int := 0;
    counter int;
BEGIN
    FOREACH n_row IN ARRAY rows
    LOOP
        EXECUTE format('INSERT INTO t_random_rows (A, B, C, D, E) SELECT random()*100, random()*100, random()*100, random()*100, random()*100 from generate_series(1, %s);', n_row-n_row_prev);
        n_row_prev := n_row;
        FOR counter in 1..(repetitions)
        LOOP
            _start_ts := clock_timestamp();
            EXECUTE format('SELECT SUM(lift5(A,B,C,D,E)) FROM t_random_rows;');
            _end_ts := clock_timestamp();
            RAISE NOTICE 'Test: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
            EXECUTE format('INSERT INTO t_stats_rows (rows, time) VALUES (%s , %s)', n_row, 1000 * (extract(epoch FROM _end_ts - _start_ts)));
        end LOOP;
    end LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test_main() RETURNS void AS $$
DECLARE
BEGIN
    PERFORM prepare_experiments();
    PERFORM test_rows(ARRAY[1000000, 10000000, 20000000, 30000000, 40000000, 50000000], 5);
    PERFORM test_cols(5);
END;
$$ LANGUAGE plpgsql;