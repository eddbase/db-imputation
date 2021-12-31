CREATE OR REPLACE FUNCTION linear_regression_model(
        c cofactor, 
        label_idx int, 
        step_size float8, 
        lambda float8, 
        max_iterations int
    )
    RETURNS float8[]
    AS :FACTML_LIBRARY, 'linear_regression_model'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
