#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "extmem.h"

#define buf_size 65
#define blk_size 8
#define hash_size 4
#define Raddr 1000000
#define Saddr 2000000
#define Linear_Search_addr 3000000
#define Binary_Search_addr 3100000
#define Index_Search_addr 3200000
#define Project_addr 4000000
#define Nest_Loop_Join_addr 5000000
#define Sort_Merge_Join_addr 5100000
#define Hash_Join_addr 5200000
#define Union_addr 6000000
#define Intersection_addr 7000000
#define Difference_addr 8000000
#define Temp_addr 9000000
#define R_merge_sort_addr 10000000
#define S_merge_sort_addr 11000000
#define R_index_addr 10100000
#define S_index_addr 11100000
#define R_hash_addr 12000000
#define S_hash_addr 13000000

int myRand(){
    return rand();
}

int initR( Buffer* buf ){
    unsigned char* blk;
    int i = 0 , j = 0;
    int rowNum = 0;
    int randNum;
    unsigned int addr = 1000000;

    /*initialize the buffer for relation*/
    if (!initBuffer(520, 64, buf))
    {
        perror("Buffer R Initialization Failed!\n");
        return -1;
    }
    /*get a new block in buffer to generate data of relation*/
    blk = getNewBlockInBuffer(buf);

    /*generate relation R*/
    for( addr = 1000000 ; addr < 1000016 ; addr++ )
    {
        /*generate each block with 7 rows*/
        for( i = 0 ; i < 7 ; i++ )
        {
            randNum = myRand()%40 + 1;
            *(int*)(blk+8*i) = randNum;
            randNum = myRand()%1000 + 1;
            *(int*)(blk+8*i+4) = randNum;
            rowNum++;

            /*test to check the relation*/
            //printf("%d,%d\t",*(int*)(blk+8*i),*(int*)(blk+8*i+4));

            /*when the row number reach 112,then stop generation*/
            if(rowNum==112)
            {
                break;
            }
        }

        if(rowNum >= 112)
        {
            *(int*)(blk+60) = 0;
            if(writeBlockToDisk(blk, addr, buf) != 0)
            {
                perror("Writing Block Failed!\n");
                return -1;
            }
            break;
        }
        else
        {
            /*add next address into current block*/
            *(int*)(blk+60) = addr+1;

            /*write current block into disk*/
            if(writeBlockToDisk(blk, addr, buf) != 0)
            {
                perror("Writing Block Failed!\n");
                return -1;
            }
        }
    }
    freeBlockInBuffer(blk,buf);
    return 0;
}

int initS( Buffer* buf ){
    unsigned char* blk;
    int i = 0 , j = 0;
    int rowNum = 0;
    int randNum;
    unsigned int addr = 2000000;

    /*initialize the buffer for relation*/
    if (!initBuffer(520, 64, buf))
    {
        perror("Buffer S Initialization Failed!\n");
        return -1;
    }
    /*get a new block in buffer to generate data of relation*/
    blk = getNewBlockInBuffer(buf);

    /*generate relation S*/
    for( addr = 2000000 ; addr < 2000032 ; addr++ )
    {
        /*generate each block with 7 rows*/
        for( i = 0 ; i < 7 ; i++ )
        {
            randNum = myRand()%41 + 20;
            *(int*)(blk+8*i) = randNum;
            randNum = myRand()%1000 + 1;
            *(int*)(blk+8*i+4) = randNum;
            rowNum++;

            /*test to check the relation*/
            //printf("%d,%d\t",*(int*)(blk+8*i),*(int*)(blk+8*i+4));

            /*when the row number reach 112,then stop generation*/
            if(rowNum==224)
            {
                break;
            }
        }

        if(rowNum >= 224)
        {
            *(int*)(blk+60) = 0;
            if(writeBlockToDisk(blk, addr, buf) != 0)
            {
                perror("Writing Block Failed!\n");
                return -1;
            }
            break;
        }
        else
        {
            /*add next address into current block*/
            *(int*)(blk+60) = addr+1;

            /*write current block into disk*/
            if(writeBlockToDisk(blk, addr, buf) != 0)
            {
                perror("Writing Block Failed!\n");
                return -1;
            }
        }
    }
    freeBlockInBuffer(blk,buf);
    return 0;
}

int readRelation(unsigned int addr){
    //printf("read data from %d\n",addr);
    Buffer buf;
    unsigned char* blk;
    int i = 0;

    /*initialize the buffer for relation*/
    if (!initBuffer(520, 64, &buf))
    {
        perror("Buffer R Initialization Failed!\n");
        return -1;
    }

    while(addr!=0)
    {
        /* Get a new block in the buffer */
        //blk = getNewBlockInBuffer(&buf);

        /* Read the block from the hard disk */
        if ((blk = readBlockFromDisk(addr, &buf)) == NULL)
        {
            perror("readRelation-Reading Block Failed!\n");
            return -1;
        }

        printf("addr::%d\t",addr);

        /*read address of next block*/
        addr = *(int*)(blk+60);

        /*print data in the block*/
        for( i = 0 ; i < 7 ; i++ )
        {
            printf("%d,%d\t",*(int*)(blk+i*8),*(int*)(blk+i*8+4));
        }
        printf("\n");
        freeBlockInBuffer(blk,&buf);
    }
    printf("\n");
    freeBuffer(&buf);
}

int readProjection(unsigned int addr) {
	//printf("read data from %d\n",addr);
	Buffer buf;
	unsigned char* blk;
	int i = 0;

	/*initialize the buffer for relation*/
	if (!initBuffer(520, 64, &buf))
	{
		perror("Buffer R Initialization Failed!\n");
		return -1;
	}

	while (addr != 0)
	{
		/* Get a new block in the buffer */
		//blk = getNewBlockInBuffer(&buf);

		/* Read the block from the hard disk */
		if ((blk = readBlockFromDisk(addr, &buf)) == NULL)
		{
			perror("readRelation-Reading Block Failed!\n");
			return -1;
		}

		printf("addr::%d\t", addr);

		/*read address of next block*/
		addr = *(int*)(blk + 60);

		/*print data in the block*/
		for (i = 0; i < blk_size*2-1; i++)
		{
			printf("%d\t", *(int*)(blk + i * 4));
		}
		printf("\n");
		freeBlockInBuffer(blk, &buf);
	}
	printf("\n");
	freeBuffer(&buf);
}

int readJoin(unsigned int addr) {
	//printf("read data from %d\n",addr);
	Buffer buf;
	unsigned char* blk;
	int i = 0;

	/*initialize the buffer for relation*/
	if (!initBuffer(520, 64, &buf))
	{
		perror("Buffer R Initialization Failed!\n");
		return -1;
	}

	while (addr != 0)
	{
		/* Get a new block in the buffer */
		//blk = getNewBlockInBuffer(&buf);

		/* Read the block from the hard disk */
		if ((blk = readBlockFromDisk(addr, &buf)) == NULL)
		{
			perror("readRelation-Reading Block Failed!\n");
			return -1;
		}

		printf("addr::%d\t", addr);

		/*read address of next block*/
		addr = *(int*)(blk + 60);

		/*print data in the block*/
		for (i = 0; i < blk_size * 2 /3; i++)
		{
			printf("%d,%d,%d\t", *(int*)(blk + i * 12), *(int*)(blk + i * 12 + 4), *(int*)(blk + i * 12 + 8));
		}
		printf("\n");
		freeBlockInBuffer(blk, &buf);
	}
	printf("\n");
	freeBuffer(&buf);
}

int readIndex(int addr)
{
	Buffer buf;
	unsigned char* blk;
	int i;

	/*initialize the buffer for relation*/
	if (!initBuffer(buf_size * 8, blk_size * 8, &buf))
	{
		perror("Buffer Initialization Failed!\n");
		return -1;
	}

	while (addr != 0)
	{
		/* Read the block from the hard disk */
		if ((blk = readBlockFromDisk(addr, &buf)) == NULL)
		{
			perror("readRelation-Reading Block Failed!\n");
			return -1;
		}
		for (i = 0; i < blk_size; i++)
		{
			printf("%d,%d,%d\t", *(unsigned short int*)(blk + i * 8), *(int*)(blk + i * 8 + 2), *(unsigned short int*)(blk + i * 8 + 6));
		}
		printf("\n");
		addr = *(int*)(blk + 60);
		freeBlockInBuffer(blk, &buf);
	}
	freeBuffer(&buf);
}

int Merge_Sort(int addr,int sort_column,int save_addr){

    Buffer buf;
    unsigned char* first_input_blk, *second_input_blk, *output_blk;
    int i, j, k, currentaddr, current_save_addr, fileNum, output_size, first_seq, second_seq, first_block_seq, second_block_seq;

    /*initialize the buffer for relation*/
    if (!initBuffer(520, 64, &buf))
    {
        perror("Buffer R Initialization Failed!\n");
        return -1;
    }
    /*get a new block in buffer to generate data of relation*/
    output_blk = getNewBlockInBuffer(&buf);

    /*count the number of files that save current relation*/
    currentaddr = addr;
    fileNum = 0;
    while(currentaddr!=0)
    {
        fileNum++;
        if ((first_input_blk = readBlockFromDisk(currentaddr, &buf)) == NULL)
        {
            perror("Sort_Merge-Reading Block Failed!\n");
            return -1;
        }
        currentaddr = *(int*)(first_input_blk+60);
        freeBlockInBuffer(first_input_blk,&buf);
    }

    /*record address of each file of the relation*/
    i = 0;
    int *addrs = (int*)malloc(fileNum*sizeof(int));
    currentaddr = addr;
    while(currentaddr!=0)
    {
        addrs[i++] = currentaddr;
        if ((first_input_blk = readBlockFromDisk(currentaddr, &buf)) == NULL)
        {
            perror("Sort_Merge-Reading Block Failed!\n");
            return -1;
        }
        currentaddr = *(int*)(first_input_blk+60);
        freeBlockInBuffer(first_input_blk,&buf);
    }

    /*merge sort*/
    /*sort in each block*/
    current_save_addr = save_addr;
    int *save_addrs = (int*)malloc(fileNum*sizeof(int));
    for( i = 0 ; i < fileNum ; i++ )
    {
        int temp[2];
        int min;
        if ((first_input_blk = readBlockFromDisk(addrs[i], &buf)) == NULL)
        {
            perror("Sort_Merge-Reading Block Failed!\n");
            return -1;
        }
        for( j = 0 ; j < 7 ; j ++ )
        {
            min = j;
            for( k = j+1 ; k < 7 ; k++ )
            {
                if( *(int*)(first_input_blk+8*k+(sort_column-1)*4) < *(int*)(first_input_blk+8*min+(sort_column-1)*4))
                {
                    min = k;
                }
            }
            /*switch*/
            if(min != j)
            {
                temp[0] = *(int*)(first_input_blk+8*j);
                temp[1] = *(int*)(first_input_blk+8*j+4);
                *(int*)(first_input_blk+8*j) = *(int*)(first_input_blk+8*min);
                *(int*)(first_input_blk+8*j+4) = *(int*)(first_input_blk+8*min+4);
                *(int*)(first_input_blk+8*min) = temp[0];
                *(int*)(first_input_blk+8*min+4) = temp[1];
            }
        }
        /*write block into hard disk*/
        if(i!=fileNum-1)
        {
            *(int*)(first_input_blk+60) = current_save_addr+1;
        }
        else
        {
            *(int*)(first_input_blk+60) = 0;
        }
        if(writeBlockToDisk(first_input_blk, current_save_addr, &buf) != 0)
        {
            perror("Writing Block Failed!\n");
            return -1;
        }
        save_addrs[i] = current_save_addr;
        current_save_addr++;
        //freeBlockInBuffer(first_input_blk,&buf);
    }

	/*test middle result(after in-block sort)*/
    //readRelation(R_sort_merger_addr);

    /*merge sort among blocks*/
	/*current save address is a temp address,which needs to reload to original address*/
	unsigned char* temp_blk;
	for( i = 1 ; i <= fileNum/2 ; i *= 2 )
    {
        output_size = 0;
        current_save_addr = Temp_addr;
        first_seq = second_seq = 0;
		
		/*sort the whole files with i files once*/
        for( j = 0 ; j < fileNum ; j += 2*i )
        {
			first_block_seq = second_block_seq = 0;
			if ((first_input_blk = readBlockFromDisk(save_addrs[j], &buf)) == NULL)
            {
                perror("Sort_Merge-Reading Block Failed!\n");
                return -1;
            }
            first_block_seq++;

            if ((second_input_blk = readBlockFromDisk(save_addrs[j+i], &buf)) == NULL)
            {
                perror("Sort_Merge-Reading Block Failed!\n");
                return -1;
            }
            second_block_seq++;

            /*sort in the block*/
            while( first_block_seq <= i && second_block_seq <= i && j+i+second_block_seq <= fileNum )
            {
                /*output block is full,write it in the hard disk*/
                if(output_size>=7)
                {
                    *(int*)(output_blk+60) = current_save_addr+1;
                    if(writeBlockToDisk(output_blk, current_save_addr, &buf) != 0)
                    {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
					output_blk = getNewBlockInBuffer(&buf);
					output_size = 0;
                    current_save_addr++;
                }

                if( *(int*)(first_input_blk+first_seq*8+(sort_column-1)*4) < *(int*)(second_input_blk+second_seq*8+(sort_column-1)*4) )
                {
                    *(int*)(output_blk+output_size*8) = *(int*)(first_input_blk+first_seq*8);
                    *(int*)(output_blk+output_size*8+4) = *(int*)(first_input_blk+first_seq*8+4);
                    first_seq++;
                    output_size++;
                    if(first_seq >= 7)
                    {
                        if(first_block_seq < i)
                        {
                            freeBlockInBuffer(first_input_blk,&buf);
                            if ((first_input_blk = readBlockFromDisk(save_addrs[j+first_block_seq], &buf)) == NULL)
                            {
                                perror("Sort_Merge-Reading Block Failed!\n");
                                return -1;
                            }
                        }
                        first_block_seq++;
                        first_seq = 0;
                    }
                }
                else
                {
                    *(int*)(output_blk+output_size*8) = *(int*)(second_input_blk+second_seq*8);
                    *(int*)(output_blk+output_size*8+4) = *(int*)(second_input_blk+second_seq*8+4);
                    second_seq++;
                    output_size++;
                    if(second_seq >= 7)
                    {
                        if(second_block_seq < i)
                        {
                            freeBlockInBuffer(second_input_blk,&buf);
                            if ((second_input_blk = readBlockFromDisk(save_addrs[j+i+second_block_seq], &buf)) == NULL)
                            {
                                perror("Sort_Merge-Reading Block Failed!\n");
                                return -1;
                            }
                        }
                        second_block_seq++;
                        second_seq = 0;
                    }
                }
            }
            while( first_block_seq <= i )
            {
                /*output block is full,write it in the hard disk*/
                if(output_size>=7)
                {
                    *(int*)(output_blk+60) = current_save_addr+1;
                    if(writeBlockToDisk(output_blk, current_save_addr, &buf) != 0)
                    {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
					output_blk = getNewBlockInBuffer(&buf);
                    output_size = 0;
                    current_save_addr++;
                }

                *(int*)(output_blk+output_size*8) = *(int*)(first_input_blk+first_seq*8);
                *(int*)(output_blk+output_size*8+4) = *(int*)(first_input_blk+first_seq*8+4);
                first_seq++;
                output_size++;
                if(first_seq >= 7)
                {
                    if(first_block_seq < i)
                    {
                        freeBlockInBuffer(first_input_blk,&buf);
                        if ((first_input_blk = readBlockFromDisk(save_addrs[j+first_block_seq], &buf)) == NULL)
                        {
                            perror("Sort_Merge-Reading Block Failed!\n");
                            return -1;
                        }
                    }
                    first_block_seq++;
                    first_seq = 0;
                }
            }
            while( second_block_seq <= i && j+i+second_block_seq <= fileNum)
            {
                /*output block is full,write it in the hard disk*/
                if(output_size>=7)
                {
                    *(int*)(output_blk+60) = current_save_addr+1;
                    if(writeBlockToDisk(output_blk, current_save_addr, &buf) != 0)
                    {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
					output_blk = getNewBlockInBuffer(&buf);
                    output_size = 0;
                    current_save_addr++;
                }

                *(int*)(output_blk+output_size*8) = *(int*)(second_input_blk+second_seq*8);
                *(int*)(output_blk+output_size*8+4) = *(int*)(second_input_blk+second_seq*8+4);
                second_seq++;
                output_size++;
                if(second_seq >= 7)
                {
                    if(second_block_seq < i && j+i+second_block_seq < fileNum-1 )
                    {
                        freeBlockInBuffer(second_input_blk,&buf);
                        if ((second_input_blk = readBlockFromDisk(save_addrs[j+i+second_block_seq], &buf)) == NULL)
                        {
                            perror("Sort_Merge-Reading Block Failed!\n");
                            return -1;
                        }
                    }
                    second_block_seq++;
                    second_seq = 0;
                }
			}
			freeBlockInBuffer(first_input_blk, &buf);
			freeBlockInBuffer(second_input_blk, &buf);
		}

		/*save last output buffer*/
		for (; output_size < 7; output_size)
		{
			*(int*)(output_blk + 8 * output_size) = 0;
			*(int*)(output_blk + 8 * output_size + 4) = 0;
		}
		*(int*)(output_blk + 60) = 0;
		if (writeBlockToDisk(output_blk, current_save_addr, &buf) != 0)
		{
			perror("Writing Block Failed!\n");
			return -1;
		}
		output_blk = getNewBlockInBuffer(&buf);

		/*move data from temp place to original place*/
		for (j = Temp_addr , k = 0; j <= current_save_addr; j++, k++)
		{
			if ((temp_blk = readBlockFromDisk(j, &buf)) == NULL)
			{
				perror("Sort_Merge-Reading Block Failed!\n");
				return -1;
			}
			if (k < fileNum-1)
			{
				*(int*)(temp_blk + 60) = save_addrs[k + 1];
			}
			else
			{
				*(int*)(temp_blk + 60) = 0;
			}
			if (writeBlockToDisk(temp_blk, save_addrs[k], &buf) != 0)
			{
				perror("Writing Block Failed!\n");
				return -1;
			}
		}

		/*test middle result(after each time of the whole files' sort)*/
		//printf("--------i=%d--------\n",i);
        //readRelation(R_sort_merger_addr);
    }


    freeBuffer(&buf);
}

/*key = n mod 4*/
int Hash(int addr, int hash_column, int save_addr)
{
	Buffer buf;
	unsigned char * hash_blk[hash_size], *input_blk;
	int i, j, current_addr;
	int hash_addr[hash_size], hash_current_addr[hash_size], hash_bucket_size[hash_size];

	/*initialize hash addresses and size*/
	for (i = 0; i < hash_size; i++)
	{
		hash_current_addr[i] = hash_addr[i] = save_addr + i;
		hash_bucket_size[i] = 0;
	}

	/*initialize the buffer for relation*/
	if (!initBuffer(buf_size*8, blk_size*8, &buf))
	{
		perror("Buffer R Initialization Failed!\n");
		return -1;
	}
	/*get a new block in buffer to generate data of relation*/
	for (i = 0; i < hash_size ; i++)
	{
		hash_blk[i] = getNewBlockInBuffer(&buf);
	}

	current_addr = addr;
	while(current_addr!=0)
	{
		/* Read the block from the hard disk */
		if ((input_blk = readBlockFromDisk(current_addr, &buf)) == NULL)
		{
			perror("readRelation-Reading Block Failed!\n");
			return -1;
		}

		for (i = 0; i < 7; i++)
		{
			j = *(int*)(input_blk + i * 8 + (hash_column - 1) * 4) % hash_size;

			/*when the bucket is full, write it in the hard disk*/
			/*and apply for a new block*/
			if (hash_bucket_size[j] >= 7)
			{
				*(int*)(hash_blk[j] + 60) = hash_current_addr[j] + hash_size;
				if (writeBlockToDisk(hash_blk[j], hash_current_addr[j], &buf) != 0)
				{
					perror("Writing Block Failed!\n");
					return -1;
				}
				hash_current_addr[j] += hash_size;
				hash_blk[j] = getNewBlockInBuffer(&buf);
				hash_bucket_size[j] = 0;
			}

			*(int*)(hash_blk[j] + hash_bucket_size[j] * 8) = *(int*)(input_blk + i * 8);
			*(int*)(hash_blk[j] + hash_bucket_size[j] * 8 + 4) = *(int*)(input_blk + i * 8 + 4);
			hash_bucket_size[j]++;
		}
		current_addr = *(int*)(input_blk + 60);
		freeBlockInBuffer(input_blk, &buf);
	}

	/*pad the rest of each bucket with 0, and save them in the hard disk*/
	for (i = 0; i < hash_size; i++)
	{
		for (j = hash_bucket_size[i]; j < blk_size; j++)
		{
			*(int*)(hash_blk[i] + j * 8) = 0;
			*(int*)(hash_blk[i] + j * 8 + 4) = 0;
		}
		if (writeBlockToDisk(hash_blk[i], hash_current_addr[i], &buf) != 0)
		{
			perror("Writing Block Failed!\n");
			return -1;
		}
	}
	freeBuffer(&buf);
}

void prompt(){
    printf("1.线性搜索算法\n");
    printf("2.二元搜索算法\n");
    printf("3.索引算法\n");
    printf("4.投影算法\n");
    printf("5.Nest-Loop-Join\n");
    printf("6.Sort-Merge-Join\n");
    printf("7.Hast-Join\n");
    printf("8.并\n");
    printf("9.交\n");
    printf("10.差\n");
    printf("11.exit\n");
}

//线性搜索算法
void Linear_Search()
{
    Buffer buf;
    unsigned char* input_blk;
    unsigned char* output_blk;

    int addr;
    int save_addr = Linear_Search_addr;
    int i , j;

    /*initialize the buffer for relation*/
    if (!initBuffer(520, 64, &buf))
    {
        perror("Buffer R Initialization Failed!\n");
        return -1;
    }
    /*get a new block in buffer to generate data of relation*/
    output_blk = getNewBlockInBuffer(&buf);

    printf("--------R.A=40--------\n");
    addr = Raddr;
    j = 0;
    while(addr!=0){
        if ((input_blk = readBlockFromDisk(addr, &buf)) == NULL)
        {
            printf("R Relation-Reading Block Failed!(addr:%d)\n",addr);
            return -1;
        }
        addr = *(int*)(input_blk+60);
        for( i = 0 ; i < 7 ; i++ )
        {
            if( *(int*)(input_blk+8*i) == 40 )
            {
                printf("%d,%d\t",*(int*)(input_blk+8*i),*(int*)(input_blk+8*i+4));
                if(j==7)
                {
                    *(int*)(output_blk+60) = save_addr+1;
                    if(writeBlockToDisk(output_blk, save_addr, &buf) != 0)
                    {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
					output_blk = getNewBlockInBuffer(&buf);
                    j = 0;
                    save_addr++;
                }
                *(int*)(output_blk+8*j) = *(int*)(input_blk+8*i);
                *(int*)(output_blk+8*j+4) = *(int*)(input_blk+8*i+4);
                j++;

            }
        }
        freeBlockInBuffer(input_blk,&buf);
    }
	printf("\n");

    printf("--------S.C=60--------\n");
    addr = Saddr;
    while(addr!=0){
        if ((input_blk = readBlockFromDisk(addr, &buf)) == NULL)
        {
            printf("S Relation-Reading Block Failed!(addr:%d)\n",addr);
            return -1;
        }
        addr = *(int*)(input_blk+60);
        for( i = 0 ; i < 7 ; i++ )
        {
            if( *(int*)(input_blk+8*i) == 60 )
            {
                printf("%d,%d\t",*(int*)(input_blk+8*i),*(int*)(input_blk+8*i+4));
                if(j==7)
                {
                    *(int*)(output_blk+60) = save_addr+1;
                    if(writeBlockToDisk(output_blk, save_addr, &buf) != 0)
                    {
                        perror("Writing Block Failed!\n");
                        return -1;
                    }
					output_blk = getNewBlockInBuffer(&buf);
                    j = 0;
                    save_addr++;
                }
                *(int*)(output_blk+8*j) = *(int*)(input_blk+8*i);
                *(int*)(output_blk+8*j+4) = *(int*)(input_blk+8*i+4);
                j++;
            }
        }
        freeBlockInBuffer(input_blk,&buf);
    }
	printf("\n");

    for( ; j < 7 ; j++ )
    {
        *(int*)(output_blk+8*j) = 0;
        *(int*)(output_blk+8*j+4) = 0;
    }
    *(int*)(output_blk+60) = 0;
    if(writeBlockToDisk(output_blk, save_addr, &buf) != 0)
    {
        perror("Writing Block Failed!\n");
        return -1;
    }
}

//二元搜索算法
void Binary_Search()
{

}

/*parse index*/
/*index key(2),address(4),position(2)*/
void create_index(int addr,int index_column,int save_addr)
{
	Buffer buf;
	unsigned char* input_blk, *output_blk;
	int i, j, current_addr, index;

	/*initialize the buffer for relation*/
	if (!initBuffer(buf_size * 8, blk_size * 8, &buf))
	{
		perror("Buffer R Initialization Failed!\n");
		return -1;
	}
	/*get a new block in buffer to generate data of relation*/
	output_blk = getNewBlockInBuffer(&buf);

	for (i = 0; i < blk_size - 1; i++)
	{
		*(unsigned short int*)(output_blk + i * 8) = i * 10;
	}

	index = 0;
	current_addr = addr;
	while (current_addr != 0)
	{
		/* Read the block from the hard disk */
		if ((input_blk = readBlockFromDisk(current_addr, &buf)) == NULL)
		{
			perror("readRelation-Reading Block Failed!\n");
			return -1;
		}

		for (i = 0; i < blk_size - 1; i++)
		{
			/*if (index > 7)
			{
				*(int*)(output_blk + 60) = save_addr + 1;
				if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
				{
					perror("Writing Block Failed!\n");
					return -1;
				}
				save_addr++;
				output_blk = getNewBlockInBuffer(&buf);
				index = 0;
			}*/

			if (*(int*)(input_blk + i * 8 + (index_column - 1) * 4) / 10 >= index)
			{
				index = *(int*)(input_blk + i * 8 + (index_column - 1) * 4) / 10;
				*(int*)(output_blk + index * 8 + 2) = current_addr;
				*(unsigned short int*)(output_blk + index * 8 + 6) = i;
				index++;
			}
		}

		current_addr = *(int*)(input_blk + 60);
		freeBlockInBuffer(input_blk, &buf);
	}
	for (i = index; i < blk_size; i++)
	{
		*(int*)(output_blk + i * 8 + 2) = 0;
		*(unsigned short int*)(output_blk + i * 8 + 6) = 0;
	}
	if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
	{
		perror("Writing Block Failed!\n");
		return -1;
	}
	freeBuffer(&buf);
}

//索引算法
void Index_Search()
{
	Buffer buf;
	unsigned char* index_blk, *input_blk, *output_blk;
	int index_addr, i, target_addr, pos, save_addr, output_size;

	/*initialize the buffer for relation*/
	if (!initBuffer(buf_size * 8, blk_size * 8, &buf))
	{
		perror("Buffer R Initialization Failed!\n");
		return -1;
	}
	/*get a new block in buffer to generate data of relation*/
	output_blk = getNewBlockInBuffer(&buf);
	
	/*R.A = 40*/
	printf("--------R.A=40--------\n");
	index_addr = R_index_addr;
	target_addr = 0;
	pos = 0;
	save_addr = Index_Search_addr;
	output_size = 0;
	/*find data position in index files*/
	while (index_addr != 0)
	{
		if ((input_blk = readBlockFromDisk(index_addr, &buf)) == NULL)
		{
			perror("Reading Block Failed!\n");
			return -1;
		}
		for (i = 0; i < blk_size - 1; i++) 
		{
			if (*(unsigned short int*)(input_blk + 8 * i) == 40)
			{
				target_addr = *(int*)(input_blk + 8 * i + 2);
				pos = *(unsigned short int*)(input_blk + 8 * i + 6);
				break;
			}
		}
		index_addr = *(int*)(input_blk + 60);
		freeBlockInBuffer(input_blk, &buf);
	}

	/*search data with index*/
	if (target_addr == 0)
	{
		printf("data not found");
		for (i = 0; i < blk_size * 2; i++)
		{
			*(int*)(output_blk + i * 4) = 0;
		}
	}
	else
	{
		i = pos;
		while (target_addr != 0 && pos != -1)
		{
			if ((input_blk = readBlockFromDisk(target_addr, &buf)) == NULL)
			{
				perror("Reading Block Failed!\n");
				return -1;
			}
			for (; i < blk_size - 1; i++)
			{
				if (*(int*)(input_blk + 8 * i) == 40)
				{
					if (output_size >= 7)
					{
						*(int*)(output_size + 60) = save_addr + 1;
						if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
						{
							perror("Writing Block Failed!\n");
							return -1;
						}
						save_addr++;
						output_blk = getNewBlockInBuffer(&buf);
						output_size = 0;
					}
					printf("%d,%d\t", *(int*)(input_blk + 8 * i), *(int*)(input_blk + 8 * i + 4));
					*(int*)(output_blk + 8 * output_size) = *(int*)(input_blk + 8 * i);
					*(int*)(output_blk + 8 * output_size + 4) = *(int*)(input_blk + 8 * i + 4);
					output_size++;
				}
				else if (*(int*)(input_blk + 8 * i) > 40)
				{
					pos = -1;
				}
			}
			target_addr = *(int*)(input_blk + 60);
			freeBlockInBuffer(input_blk, &buf);
			i = 0;
		}
	}
	printf("\n");

	/*S.C = 60*/
	printf("--------S.C=60--------\n");
	index_addr = S_index_addr;
	target_addr = 0;
	pos = 0;
	/*find data position in index files*/
	while (index_addr != 0)
	{
		if ((input_blk = readBlockFromDisk(index_addr, &buf)) == NULL)
		{
			perror("Reading Block Failed!\n");
			return -1;
		}
		for (i = 0; i < blk_size - 1; i++)
		{
			if (*(unsigned short int*)(input_blk + 8 * i) == 60 )
			{
				target_addr = *(int*)(input_blk + 8 * i + 2);
				pos = *(unsigned short int*)(input_blk + 8 * i + 6);
				break;
			}
		}
		index_addr = *(int*)(input_blk + 60);
		freeBlockInBuffer(input_blk, &buf);
	}

	/*search data with index*/
	if (target_addr == 0)
	{
		printf("data not found");
		for (i = 0; i < blk_size * 2; i++)
		{
			*(int*)(output_blk + i * 4) = 0;
		}
	}
	else
	{
		i = pos;
		while (target_addr != 0 && pos != -1)
		{
			if ((input_blk = readBlockFromDisk(target_addr, &buf)) == NULL)
			{
				perror("Reading Block Failed!\n");
				return -1;
			}
			for (; i < blk_size - 1; i++)
			{
				if (*(int*)(input_blk + 8 * i) == 60)
				{
					if (output_size >= 7)
					{
						*(int*)(output_size + 60) = save_addr + 1;
						if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
						{
							perror("Writing Block Failed!\n");
							return -1;
						}
						save_addr++;
						output_blk = getNewBlockInBuffer(&buf);
						output_size = 0;
					}
					printf("%d,%d\t", *(int*)(input_blk + 8 * i), *(int*)(input_blk + 8 * i + 4));
					*(int*)(output_blk + 8 * output_size) = *(int*)(input_blk + 8 * i);
					*(int*)(output_blk + 8 * output_size + 4) = *(int*)(input_blk + 8 * i + 4);
					output_size++;
				}
				else if (*(int*)(input_blk + 8 * i) > 40)
				{
					pos = -1;
				}
			}
			target_addr = *(int*)(input_blk + 60);
			freeBlockInBuffer(input_blk, &buf);
			i = 0;
		}
	}	
	printf("\n");

	for (i = output_size; i < blk_size; i++)
	{
		*(int*)(output_blk + 8 * i) = 0;
		*(int*)(output_blk + 8 * i + 4) = 0;
	}
	if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
	{
		perror("Writing Block Failed!\n");
		return -1;
	}
	freeBuffer(&buf);
}

//投影
void Projection()
{
	Buffer buf;
	unsigned char* input_blk, *output_blk;
	int i, output_index, current_addr, current_save_addr;

	/*initialize the buffer for relation*/
	if (!initBuffer(buf_size * 8, blk_size * 8, &buf))
	{
	perror("Buffer R Initialization Failed!\n");
	return -1;
	}
	/*get a new block in buffer to generate data of relation*/
	output_blk = getNewBlockInBuffer(&buf);

	current_addr = Raddr;
	current_save_addr = Project_addr;
	output_index = 0;
	while (current_addr != 0)
	{
		/* Read the block from the hard disk */
		if ((input_blk = readBlockFromDisk(current_addr, &buf)) == NULL)
		{
			perror("readRelation-Reading Block Failed!\n");
			return -1;
		}

		for (i = 0; i < blk_size - 1; i++)
		{
			if (output_index >= blk_size * 2 - 1)
			{
				*(int*)(output_blk + 60) = current_save_addr + 1;
				if (writeBlockToDisk(output_blk, current_save_addr, &buf) != 0)
				{
					perror("Writing Block Failed!\n");
					return -1;
				}
				current_save_addr++;
				output_blk = getNewBlockInBuffer(&buf);
				output_index = 0;
			}
			*(int*)(output_blk + output_index * 4) = *(int*)(input_blk + i * 8);
			output_index++;
		}

		current_addr = *(int*)(input_blk + 60);
		freeBlockInBuffer(input_blk, &buf);
	}
	for (i = output_index; i < blk_size * 2; i++)
	{
		*(int*)(output_blk + 4 * i) = 0;
	}
	if (writeBlockToDisk(output_blk, current_save_addr, &buf) != 0)
	{
		perror("Writing Block Failed!\n");
		return -1;
	}
	freeBuffer(&buf);
}

/*Buffer buf;
unsigned char* blk;

/*initialize the buffer for relation*
if (!initBuffer(buf_size * 8, blk_size * 8, &buf))
{
perror("Buffer R Initialization Failed!\n");
return -1;
}
/*get a new block in buffer to generate data of relation*
blk = getNewBlockInBuffer(&buf);

/* Read the block from the hard disk *
if ((blk = readBlockFromDisk(addr, &buf)) == NULL)
{
perror("readRelation-Reading Block Failed!\n");
return -1;
}
if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
{
perror("Writing Block Failed!\n");
return -1;
}
save_addr++;
output_blk = getNewBlockInBuffer(&buf);
output_size = 0;
freeBlockInBuffer(blk, &buf);
freeBuffer(&buf);
*/

void Nest_Loop_Join()
{
	Buffer buf;
	/*initialize the buffer for relation*/
	if (!initBuffer(buf_size * 8, blk_size * 8, &buf))
	{
		perror("Buffer R Initialization Failed!\n");
		return -1;
	}
	int all_blk = (&buf)->numAllBlk - 2;
	
	unsigned char* output_blk, *R_input_blk;
	unsigned char** S_input_blk = (unsigned char**)malloc(all_blk*sizeof(unsigned char*));
	int i, j, k, S_num, R_next_addr, S_next_addr, save_addr, output_size;

	/*get a new block in buffer to generate data of relation*/
	output_blk = getNewBlockInBuffer(&buf);

	S_next_addr = Saddr;
	save_addr = Nest_Loop_Join_addr;
	output_size = 0;
	while (S_next_addr != 0)
	{
		/*read part of S from disk*/
		for (S_num = 0; S_num < all_blk; S_num++)
		{
			if ((S_input_blk[S_num] = readBlockFromDisk(S_next_addr, &buf)) == NULL)
			{
				perror("readRelation-Reading Block Failed!\n");
				return -1;
			}
			S_next_addr = *(int*)(S_input_blk[S_num] + 60);
			if (S_next_addr == 0)
			{
				break;
			}
		}

		/*for current part of S,join with all blocks of R*/
		R_next_addr = Raddr;
		while (R_next_addr != 0)
		{
			if ((R_input_blk = readBlockFromDisk(R_next_addr, &buf)) == NULL)
			{
				perror("readRelation-Reading Block Failed!\n");
				return -1;
			}

			/*for each tuple in R block*/
			for (i = 0; i < blk_size - 1; i++)
			{
				/*try to join each tuple in S blocks*/
				for (j = 0; j < S_num; j++)
				{
					for (k = 0; k < blk_size - 1; k++)
					{
						if (output_size >= blk_size * 2 / 3)
						{
							*(int*)(output_blk + 60) = save_addr + 1;
							if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
							{
								perror("Writing Block Failed!\n");
								return -1;
							}
							printf("\n");
							save_addr++;
							output_blk = getNewBlockInBuffer(&buf);
							output_size = 0;
						}

						/*two tuples that can join*/
						if (*(int*)(R_input_blk + i * 8) == *(int*)(S_input_blk[j] + k * 8))
						{
							printf("%d,%d,%d\t", *(int*)(R_input_blk + i * 8), *(int*)(R_input_blk + i * 8 + 4), *(int*)(S_input_blk[j] + k * 8 + 4));
							*(int*)(output_blk + output_size * 12) = *(int*)(R_input_blk + i * 8);
							*(int*)(output_blk + output_size * 12 + 4) = *(int*)(R_input_blk + i * 8 + 4);
							*(int*)(output_blk + output_size * 12 + 8) = *(int*)(S_input_blk[j] + k * 8 + 4);
							output_size++;
						}
					}
				}
			}

			R_next_addr = *(int*)(R_input_blk + 60);
			freeBlockInBuffer(R_input_blk, &buf);
		}

		/*release blocks that save current part of S*/
		for (i = 0; i < S_num; i++)
		{
			freeBlockInBuffer(S_input_blk[i], &buf);
		}
	}

	for (i = output_size * 3; i < blk_size * 2; i++)
	{
		*(int*)(output_blk + i * 4) = 0;
		if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
		{
			perror("Writing Block Failed!\n");
			return -1;
		}
		printf("\n");
	}

	freeBuffer(&buf);
}

void Sort_Merge_Join()
{
	Buffer buf;
	unsigned char* R_input_blk, *S_input_blk, *output_blk;
	int i, j, k, R_current_addr, S_current_addr, save_addr, output_size;
	int shared_key, notfound, R_pos, S_pos, current_pos, current_addr;

	/*initialize the buffer for relation*/
	if (!initBuffer(buf_size * 8, blk_size * 8, &buf))
	{
	perror("Buffer R Initialization Failed!\n");
	return -1;
	}
	/*get a new block in buffer to generate data of relation*/
	output_blk = getNewBlockInBuffer(&buf);

	R_current_addr = R_merge_sort_addr;
	S_current_addr = S_merge_sort_addr;
	save_addr = Sort_Merge_Join_addr;
	output_size = 0;
	notfound = 1;
	S_pos = R_pos = 0;
	shared_key = 0;
	if ((R_input_blk = readBlockFromDisk(R_current_addr, &buf)) == NULL)
	{
		perror("Reading Block Failed!\n");
		return -1;
	}
	if ((S_input_blk = readBlockFromDisk(S_current_addr, &buf)) == NULL)
	{
		perror("Reading Block Failed!\n");
		return -1;
	}
	while (R_current_addr != 0 && S_current_addr != 0)
	{
		/*find the minimum shared key,and move to the first two tuples with shared key*/
		while (notfound)
		{
			if (*(int*)(R_input_blk + R_pos * 8) < *(int*)(S_input_blk + S_pos * 8))
			{
				shared_key = *(int*)(S_input_blk + S_pos * 8);
			}
			else
			{
				shared_key = *(int*)(R_input_blk + R_pos * 8);
			}

			while (*(int*)(R_input_blk + (blk_size - 2) * 8) < shared_key)
			{
				R_current_addr = *(int*)(R_input_blk + 60);
				freeBlockInBuffer(R_input_blk, &buf);
				if ((R_input_blk = readBlockFromDisk(R_current_addr, &buf)) == NULL)
				{
					perror("Reading Block Failed!\n");
					return -1;
				}
				R_pos = 0;
			}
			while (*(int*)(S_input_blk + (blk_size - 2) * 8) < shared_key)
			{
				S_current_addr = *(int*)(S_input_blk + 60);
				freeBlockInBuffer(S_input_blk, &buf);
				if ((S_input_blk = readBlockFromDisk(S_current_addr, &buf)) == NULL)
				{
					perror("Reading Block Failed!\n");
					return -1;
				}
				S_pos = 0;
			}
			for (; R_pos < blk_size - 1; R_pos++)
			{
				if (*(int*)(R_input_blk + R_pos * 8) == shared_key)
				{
					notfound = 0;
					break;
				}
				else if(*(int*)(R_input_blk + R_pos * 8) > shared_key)
				{
					notfound = 1;
					shared_key = *(int*)(R_input_blk + R_pos * 8);
					break;
				}
			}
			for (; S_pos < blk_size - 1; S_pos++)
			{
				if (*(int*)(S_input_blk + S_pos * 8) == shared_key)
				{
					notfound = 0;
					break;
				}
				else if (*(int*)(S_input_blk + S_pos * 8) > shared_key)
				{
					notfound = 1;
					shared_key = *(int*)(S_input_blk + S_pos * 8);
					break;
				}
			}
		}
		/*begin joining with shared key*/
		while (!notfound)
		{
			current_pos = S_pos;
			current_addr = S_current_addr;
			/*for each tuple of R with shared key,join all tuples of S with shared key*/
			while (*(int*)(S_input_blk + current_pos * 8) == shared_key)
			{
				if (output_size >= blk_size*2/3)
				{
					*(int*)(output_blk + 60) = save_addr + 1;
					if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
					{
						perror("Writing Block Failed!\n");
						return -1;
					}
					printf("\n");
					save_addr++;
					output_blk = getNewBlockInBuffer(&buf);
					output_size = 0;
				}
				printf("%d,%d,%d\t", *(int*)(R_input_blk + R_pos * 8), *(int*)(R_input_blk + R_pos * 8 + 4), *(int*)(S_input_blk + current_pos * 8 + 4));
				*(int*)(output_blk + output_size * 12) = *(int*)(R_input_blk + R_pos * 8);
				*(int*)(output_blk + output_size * 12 + 4) = *(int*)(R_input_blk + R_pos * 8 + 4);
				*(int*)(output_blk + output_size * 12 + 8) = *(int*)(S_input_blk + current_pos * 8 + 4);
				output_size++;
				current_pos++;
				if (current_pos >= 7)
				{
					current_addr = *(int*)(S_input_blk + 60);
					if (current_addr == 0)
					{
						break;
					}
					freeBlockInBuffer(S_input_blk, &buf);
					if ((S_input_blk = readBlockFromDisk(current_addr, &buf)) == NULL)
					{
						perror("Reading Block Failed!\n");
						return -1;
					}
					current_pos = 0;
				}
			}
			freeBlockInBuffer(S_input_blk, &buf);
			if ((S_input_blk = readBlockFromDisk(S_current_addr, &buf)) == NULL)
			{
				perror("Reading Block Failed!\n");
				return -1;
			}
			R_pos++;
			if (R_pos >= 7)
			{
				R_current_addr = *(int*)(R_input_blk + 60);
				if (R_current_addr == 0)
				{
					break;
				}
				freeBlockInBuffer(R_input_blk, &buf);
				if ((R_input_blk = readBlockFromDisk(R_current_addr, &buf)) == NULL)
				{
					perror("Reading Block Failed!\n");
					return -1;
				}
				R_pos = 0;
			}
			if (*(int*)(R_input_blk + R_pos * 8) > shared_key)
			{
				while (*(int*)(S_input_blk + S_pos * 8) == shared_key)
				{
					S_pos++;
					if (S_pos >= 7)
					{
						S_current_addr = *(int*)(S_input_blk + 60);
						freeBlockInBuffer(S_input_blk, &buf);
						if ((S_input_blk = readBlockFromDisk(S_current_addr, &buf)) == NULL)
						{
							perror("Reading Block Failed!\n");
							return -1;
						}
						S_pos = 0;
					}
				}
				notfound = 1;
			}
		}
	}
	for (i = output_size * 3; i < blk_size * 2; i++)
	{
		*(int*)(output_blk + i * 4) = 0;
	}
	if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
	{
		perror("Writing Block Failed!\n");
		return -1;
	}
	printf("\n");
	freeBuffer(&buf);
}

void Hash_Join()
{
	Buffer buf;
	unsigned char* hash_blk, *output_blk, *input_blk;
	int i, j, k, hash_addr[hash_size], current_hash_addr, current_addr, save_addr, output_size;

	/*initialize the buffer for relation*/
	if (!initBuffer(buf_size * 8, blk_size * 8, &buf))
	{
		perror("Buffer R Initialization Failed!\n");
		return -1;
	}

	/*put R in buckets*/
	for (i = 0; i < hash_size; i++)
	{
		hash_addr[i] = R_hash_addr + i;
	}
	/*get a new block in buffer to generate data of relation*/
	output_blk = getNewBlockInBuffer(&buf);

	current_addr = Saddr;
	output_size = 0;
	save_addr = Hash_Join_addr;
	while (current_addr != 0)
	{
		if ((input_blk = readBlockFromDisk(current_addr, &buf)) == NULL)
		{
			perror("Reading Block Failed!\n");
			return -1;
		}
		for (i = 0; i < blk_size - 1; i++)
		{
			j = *(int*)(input_blk + 8 * i) % 4;
			current_hash_addr = hash_addr[j];
			while (current_hash_addr != 0)
			{
				if ((hash_blk = readBlockFromDisk(current_hash_addr, &buf)) == NULL)
				{
					perror("Reading Block Failed!\n");
					return -1;
				}

				for (k = 0; k < blk_size - 1; k++)
				{
					if (*(int*)(hash_blk + k * 8) == *(int*)(input_blk + i * 8))
					{
						if (output_size >= blk_size * 2 / 3)
						{
							*(int*)(output_blk + 60) = save_addr + 1;
							if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
							{
								perror("Writing Block Failed!\n");
								return -1;
							}
							printf("\n");
							save_addr++;
							output_blk = getNewBlockInBuffer(&buf);
							output_size = 0;
						}
						printf("%d,%d,%d\t", *(int*)(input_blk + i * 8), *(int*)(input_blk + i * 8 + 4), *(int*)(hash_blk + k * 8 + 4));
						*(int*)(output_blk + output_size * 12) = *(int*)(input_blk + i * 8);
						*(int*)(output_blk + output_size * 12 + 4) = *(int*)(input_blk + i * 8 + 4);
						*(int*)(output_blk + output_size * 12 + 8) = *(int*)(hash_blk + k * 8 + 4);
						output_size++;
					}
				}
				current_hash_addr = *(int*)(hash_blk + 60);
				freeBlockInBuffer(hash_blk, &buf);
			}
		}
		current_addr = *(int*)(input_blk + 60);
		freeBlockInBuffer(input_blk, &buf);
	}

	for (i = output_size * 3; i < blk_size * 2; i++)
	{
		*(int*)(output_blk + i * 4) = 0;
	}
	if (writeBlockToDisk(output_blk, save_addr, &buf) != 0)
	{
		perror("Writing Block Failed!\n");
		return -1;
	}
	printf("\n");

	freeBuffer(&buf);
}

void Union()
{

}

void Intersection()
{

}

void Difference()
{

}

int main(int argc,char **argv){
    srand(time(NULL));

	int choice = -1, i;

    printf("是否重新生成数据(1/0)");
    scanf("%d",&choice);
    if(choice==1){
        Buffer bufR,bufS;
        if(initR(&bufR)==-1){
            perror("Relation R Initialization Failed\n");
        }
        if(initS(&bufS)==-1){
            perror("Relation R Initialization Failed\n");
        }
        freeBuffer(&bufR);
        freeBuffer(&bufS);
    }
    printf("关系R:\n");
    readRelation(Raddr);
    Merge_Sort(Raddr,1,R_merge_sort_addr);
	create_index(R_merge_sort_addr, 1, R_index_addr);
	Hash(Raddr, 1, R_hash_addr);
	printf("----------index----------\n");
	readIndex(R_index_addr);
	printf("--------merge sort--------\n");
    readRelation(R_merge_sort_addr);
	printf("-----------hash-----------\n");
	for (i = 0; i < hash_size; i++)
	{
		readRelation(R_hash_addr + i);
	}
    printf("关系S:\n");
    readRelation(Saddr);
    Merge_Sort(Saddr,1, S_merge_sort_addr);
	create_index(S_merge_sort_addr, 1, S_index_addr);
	Hash(Saddr, 1, S_hash_addr);
	printf("----------index----------\n");
	readIndex(S_index_addr);
	printf("--------merge sort--------\n");
    readRelation(S_merge_sort_addr);
	printf("-----------hash-----------\n");
	for (i = 0; i < hash_size; i++)
	{
		readRelation(S_hash_addr + i);
	}

    while(choice!=-1){
        prompt();
        scanf("%d",&choice);
        switch(choice){
            case 1:{
                Linear_Search();
				readRelation(Linear_Search_addr);
                break;
            }
            case 2:{
                Binary_Search();
				readRelation(Binary_Search_addr);
                break;
            }case 3:{
                Index_Search();
				readRelation(Index_Search_addr);
                break;
            }case 4:{
                Projection();
				readProjection(Project_addr);
                break;
            }case 5:{
                Nest_Loop_Join();
				readJoin(Nest_Loop_Join_addr);
                break;
            }case 6:{
                Sort_Merge_Join();
				readJoin(Sort_Merge_Join_addr);
                break;
            }case 7:{
                Hash_Join();
				readJoin(Hash_Join_addr);
                break;
            }case 8:{
                Union();
				readRelation(Union_addr);
                break;
            }case 9:{
                Intersection();
				readRelation(Intersection_addr);
                break;
            }case 10:{
                Difference();
				readRelation(Difference_addr);
                break;
            }case 11:{
                choice = -1;
                break;
            }default:{
                choice = 0;
                break;
            }
        }
    }

    return 0;
}
