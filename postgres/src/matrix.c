
#include "matrix.h"
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <assert.h>


int16 m_add(Uint16 row, Uint16 col, float32*left, float32*right, float32*out)
{
	Uint16 i = 0;
	for (i = 0; i < row*col; i++)
		*(out + i) = *(left + i) + *(right + i);
	return 0;
}

int16 m_sub(Uint16 row, Uint16 col, float32*left, float32*right, float32*out)
{
	Uint16 i = 0;
	for (i = 0; i < row*col; i++)
		*(out + i) = *(left + i) - *(right + i);

	return 0;
}

int16 m_mul(float32*left, Uint16 row_left, Uint16 col_left, float32*right, Uint16 row_right, Uint16 col_right, float32*out)
{
	Uint16 i = 0;
	Uint16 j = 0;
	Uint16 k = 0;
	
    memset(out, 0, row_left*col_right*sizeof(float32));
	if (col_left != row_right)
	{
		return -1;
	}

	for (i = 0; i < row_left; i++)
	{
		for (j = 0; j < col_left; j++)
		{
			for (k = 0; k<col_right; k++)
				ELEMENT(out, col_right, i, k) += ELEMENT(left, col_left, i, j)* ELEMENT(right, col_right, j, k);
		}
	}

	return 0;
}
int16 m_mul_scale(Uint16 row,Uint16 col,float32*M,float32 a)
{
	Uint16 i, j;
	if (0 == a)
	{
        memset(M, 0, row*col*sizeof(float32));
		return 0;
	}
	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			ELEMENT(M, col, i, j) = ELEMENT(M, col, i, j)*a;
		}
	}
	return 0;
}
int16 m_chol(Uint16 row, Uint16 col, float32* M)
{
	Uint16 i, j, k;
    float32 *A_piv, *A_row, sum, tmp;
	for (k = 0; k < row; k++)
	{
		sum = ELEMENT(M, col, k, k);
		A_piv = M + k*col;
		for (j = 0; j < k; j++)
		{
			tmp = *A_piv++;
			sum -= tmp*tmp;
		}
		if (sum <= 0.0)
		{
			return -1;
		}
		ELEMENT(M, col, k, k) = sqrt(sum);
		for (i = k + 1; i < row; i++)
		{
			sum = ELEMENT(M, col, i, k);
			A_piv = M + k*col;
			A_row = M + i*col;
			for (j = 0; j < k; j++)
			{
				sum -= ELEMENT(M, col, i, j)*ELEMENT(M, col, k, j);
			}
			ELEMENT(M, col, j, i) = sum / ELEMENT(M, col, k, k);
			ELEMENT(M, col, i, j) = sum / ELEMENT(M, col, k, k);
		}
	}
	for (i = 0; i < row; i++)
	{
		for (j = 0; j < i; j++)
		{
			ELEMENT(M, col, i, j) = 0;
		}
	}
	return 0;
}
float32 m_det(float32* M, Uint16 n)
{
    float32 ans = 0;
    float32 t = 0;
    float32 temp[MAX_MAT_SIZE];
	Uint16 i, j, k, w;
	if (1 == n)
	{
		return *M;
	}
	for (i = 0; i < n; i++)
	{
		for (j = 0; j < n - 1; j++)
		{
			for (k = 0; k < n - 1; k++)
			{
				w = (k >= i) ? k + 1 : k;
				ELEMENT(temp, (n-1), j, k) = ELEMENT(M, n, (j + 1), w);
			}
		}
		t = m_det(temp, n - 1);
		if (0 == i % 2)
		{
			ans += ELEMENT(M, n, 0, i)*t;
		}
		else
		{
			ans -= ELEMENT(M, n, 0, i)*t;
		}
	}
	return ans;
}
int16 m_joint(float32* M,Uint16 n, float32*ans)
{
	Uint16 i, j, k, t;
    float32 temp[MAX_MAT_SIZE];
	if (1 == n)
	{
		ELEMENT(ans, n, 0, 0) = 1;
		return 0;
	}

	for (i = 0; i < n; i++)
	{
		for (j = 0; j < n; j++)
		{
			for (k = 0; k < n-1; k++)
			{
				for (t = 0; t < n - 1; t++)
				{
					ELEMENT(temp, (n - 1), k, t) = ELEMENT(M, n, (k >= i ? k + 1 : k), (t >= j ? t + 1 : t));
				}
			}

			ELEMENT(ans, n, j, i) = m_det(temp, (n - 1));
			if (1 == (i + j) % 2)
			{
				ELEMENT(ans, n, j, i) = -ELEMENT(ans, n, j, i);
			}
		}
	}
	return 0;

}
int16 m_inverse(float32 *M, Uint16 n,float32* M_inv)
{
	Uint16 i, j;
    float32 M_det;
    float32 M_joint[MAX_MAT_SIZE];
	m_joint(M, n, M_joint);
	M_det = m_det(M, n);
	m_mul_scale(n, n, M_joint, (1 / M_det));
	for (i = 0; i < n; i++)
	{
		for (j = 0; j < n; j++)
		{
			ELEMENT(M_inv, n, i, j) = ELEMENT(M_joint, n, i, j);
		}
	}
	return 0;
}
int16 m_trans(float32*M, Uint16 row, Uint16 col, float32 *M_tran)
{
	Uint16 i, j;
	for (i = 0; i < row; i++)
	{
		for (j = 0; j < col; j++)
		{
			ELEMENT(M_tran, row, j, i) = ELEMENT(M, col, i, j);
		}
	}
	return 0;
}
int16 m_repmat(Uint16 row, Uint16 col, float32* M, Uint16 num, Uint16 ROW_or_COL, float32* out)
{
	Uint16 i, j;
	if (num <= 0)
		return -1;
	if (1 == num)
	{
		for (i = 0; i < row; i++)
		{
			for (j = 0; j < col; j++)
				ELEMENT(out, col, i, j) = ELEMENT(M, col, i, j);
		}
			
		return 0;
	}
	if (num > 1)
	{
		if (ROW_or_COL == ROW)
		{
			for (i = 0; i < num*row; i++)
			{
				for (j = 0; j < col; j++)
					ELEMENT(out, col, i, j) = ELEMENT(M, col, (i%row), j);
			}
			
		}
		else if (ROW_or_COL == COL)
		{
			for (j = 0; j < num*col; j++)
			{
				for (i = 0; i < row; i++)
					ELEMENT(out, num*col, i, j) = ELEMENT(M, col, i, (j%col));
			}
        }
		else
			return -1;
	}
	return 0;
}


int16 m_copy(const float32* src, float32* dst, const Uint16 row, const Uint16 col)
{
	assert(src && dst);
	assert(row >= 0 && col >= 0);

	memcpy((void*)dst, (void*)src, sizeof(float32)*row*col);

	return 0;
}

int16 m_qr(const float32* mat, const Uint16 row, const Uint16 col, float32* out)
{
	int16 k = 0;
	int16 j = 0;
	int16 i = 0;
	float32 alpha = 0.0;
	float32 beta = 0.0;
	float32 lambda = 0.0;
	float32 v[24] = { 0.0 };		/* maximum size is 24x24 for input matrix @in */

	assert(mat && out);
	assert(row >= col);
	assert(row <= 24);

	memcpy(out, mat, sizeof(float32)*row*col);
	for (k = 0; k < col; k++)
	{
		memset(v, 0, sizeof(v));
		alpha = 0.0;
		beta = 0.0;
		for (j = k; j < row; j++)
		{
			alpha += ELEMENT(out, col, j, k) * ELEMENT(out, col, j, k);
			*(v + j) = ELEMENT(out, col, j, k);
		}
		alpha = sqrt(alpha) * (ELEMENT(out, col, k, k) > 0 ? -1 : 1);
		*(v + k) -= alpha;

		for (j = k; j < row; j++)
		{
			beta += *(v + j) * *(v + j);
		}

		if (fabs(beta) < 1e-4)
			return -1;

		for (j = k; j < col; j++)
		{
			lambda = 0.0;
			for (i = k; i < row; i++)
			{
				lambda += *(v + i) * ELEMENT(out, col, i, j);
			}
			lambda = -2 * lambda / beta;
			for (i = k; i < row; i++)
			{
				ELEMENT(out, col, i, j) = ELEMENT(out, col, i, j) + lambda* (*(v + i));
			}
		}
	}
	ELEMENT(out, col, row - 1, col - 1) *= -1;

	return 0;
}

int16 m_cholupdate(float32* mat, const float32* vec, const Uint16 row, const Uint16 col, const char sign)
{
	int16 i = 0, j = 0;
	int16 n = row;
	float32 R[8 * 8] = { 0.0 };
	float32 x[8 * 8] = { 0.0 };
	float32 r = 0.0; 
	float32 s = 0.0;
	float32 c = 0.0;
	float32 a = 0.0, b = 0.0;

	assert(mat && vec);
	assert(row > 0 && col > 0 && row == col);
	assert(row <= 8);

	m_copy(mat, R, n, n);
	m_copy(vec, x, 1, n);

	for (i = 0; i < n; i++)
	{
		a = ELEMENT(R, n, i, i);
		b = ELEMENT(x, n, 0, i);
		if (sign == '+')
			r = sqrt(a*a + b*b);
		else
			r = sqrt(a*a - b*b);
		c = r / a;
		s = b / a;
		c *= (a < 0 ? -1.0 : 1.0);
		r *= (a < 0 ? -1.0 : 1.0);
		ELEMENT(R, n, i, i) = r;
		if (i + 1 < n)
		{
			for (j = i+1; j < n; j++)
			{
				if (sign == '+')
					ELEMENT(R, n, i, j) = (ELEMENT(R, n, i, j) + s* ELEMENT(x, 1, j, 0)) / c;
				else
					ELEMENT(R, n, i, j) = (ELEMENT(R, n, i, j) - s* ELEMENT(x, 1, j, 0)) / c;
				ELEMENT(x, n, 0, j) = c * ELEMENT(x, n, 0, j) - s*ELEMENT(R, n, i, j);
			}
		}
	}
	m_copy(R, mat, n, n);
}

void m_print(const float32* mat, const Uint16 m, const Uint16 n, const char *prompt)
{
	int16 i = 0;
	static FILE* file = NULL;
	file = stdout;
	if (file == NULL)
	{
		file = fopen("./data/debugPrint.txt", "w");
	}
	fprintf(file, "%s:%dx%d\n", prompt, m, n);
	while (i < m*n)
	{
		fprintf(file, "%+10.08f\t", (float32)*(mat+i));
		if (++i%n == 0)
			fprintf(file, "\n");
	}
	fprintf(file, "\n");
	//fflush(file);
}