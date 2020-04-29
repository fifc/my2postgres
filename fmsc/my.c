#include "my.h"
#include "db_cfg.h"

#include <stdio.h>
#include <string.h>

MYSQL* mysql;

#define SRC_TABLE "miliao_chat_msg.chat_message_"
static int fms_move(int no) {
    //mysql_query(mysql, "drop table if exists test");
    //mysql_query(mysql, "create table test(id bigint,name varchar(100),val bigint,timestamp timestamp not null default now(),primary key(id))");
    mysql_query(mysql, "set names utf8");
    MYSQL_STMT* stmt = mysql_stmt_init(mysql);
    char buf[1 << 13];
    const char* filter = "1,2,3";
    const char* fields = "seq,`from`,`to`,type,body,style,timestamp ts";
    snprintf(buf, sizeof buf, "select %s from %s%d where timestamp>=? or"
        "(timestamp=? and `from` not in (%s)) limit 10",
        fields, SRC_TABLE, no, filter);
    if (mysql_stmt_prepare(stmt, buf, strlen(buf))) {
        fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
        return 1;
    }
    /* Fetch result set meta information */
    MYSQL_RES* meta_res = mysql_stmt_result_metadata(stmt);
    if (!meta_res) {
        fprintf(stderr, " mysql_stmt_result_metadata(),returned no meta information\n");
        fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
        return 2;
    }

    printf("param count: %i\n", mysql_stmt_param_count(stmt));

    /* Get total columns in the query */
    int column_num = mysql_num_fields(meta_res);
    fprintf(stdout, " total columns in SELECT statement: %d\n", column_num);

    if (column_num != 7) { /* validate column count */
        fprintf(stderr, " invalid column count returned by MySQL\n");
        //return 3;
    }

    MYSQL_BIND bind[7];
    memset(&bind, 0, sizeof bind);
    //MYSQL_TIME ts;
    long long ts;
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = (char*)&ts;
    bind[0].is_null = 0;
    bind[0].length = 0;

    bind[1] = bind[0];

    if (mysql_stmt_bind_param(stmt, bind)) {
        fprintf(stderr, " mysql_stmt_bind_param() failed\n");
        fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
        return 4;
    }

    //ts.year = 2000;
    //ts.month = 1;
    //ts.day = 7;
    //ts.hour = 7;
    //ts.minute = 7;
    //ts.second = 7;
    //ts.second_part = 7;
    //ts.neg = false;
    ts = 1574409421896L;

    if (mysql_stmt_execute(stmt)) {
        fprintf(stderr, " mysql_stmt_execute error: %s\n", mysql_stmt_error(stmt));
        return 5;
    }

    /* Get the total rows affected */
    unsigned long numrows = mysql_stmt_affected_rows(stmt);
    fprintf(stdout, " affected rows: %lu\n", numrows);

#define FIELD_NUM 7
    bool null[FIELD_NUM];
    bool error[FIELD_NUM];
    unsigned long len[FIELD_NUM];
    int style;
    short type;
    long long seq, from, to;

    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = (char*)&seq;
    bind[0].buffer_length = sizeof seq;
    bind[0].is_null = &null[0];
    bind[0].length = &len[0];
    bind[0].error = &error[0];

    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = (char*)&from;
    bind[1].buffer_length = sizeof from;
    bind[1].is_null = &null[1];
    bind[1].length = &len[1];
    bind[1].error = &error[1];

    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = (char*)&to;
    bind[2].buffer_length = sizeof to;
    bind[2].is_null = &null[2];
    bind[2].length = &len[2];
    bind[2].error = &error[2];

    bind[3].buffer_type = MYSQL_TYPE_TINY;
    bind[3].buffer = (char*)&type;
    bind[3].buffer_length = sizeof type;
    bind[3].is_null = &null[3];
    bind[3].length = &len[3];
    bind[3].error = &error[3];

    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = buf;
    bind[4].buffer_length = sizeof buf;
    bind[4].is_null = &null[4];
    bind[4].length = &len[4];
    bind[4].error = &error[4];

    bind[5].buffer_type = MYSQL_TYPE_LONG;
    bind[5].buffer = (char*)&style;
    bind[5].buffer_length = sizeof style;
    bind[5].is_null = &null[5];
    bind[5].length = &len[5];
    bind[5].error = &error[5];

    bind[6].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[6].buffer = (char*)&ts;
    //bind[6].buffer_length = sizeof ts;
    bind[6].is_null = &null[6];
    bind[6].length = &len[6];
    bind[6].error = &error[6];

    /* Bind the result buffers */
    if (mysql_stmt_bind_result(stmt, bind)) {
        fprintf(stderr, " mysql_stmt_bind_result() failed: %s\n", mysql_stmt_error(stmt));
        return 6;
    }

    /* Now buffer all results to client (optional step) */
    if (mysql_stmt_store_result(stmt)) {
        fprintf(stderr, " mysql_stmt_store_result() failed\n");
        fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
        return 7;
    }

    int row_count = 0;
    fprintf(stdout, "Fetching results ...\n");
    int ret = 0;
    while ((ret = mysql_stmt_fetch(stmt)) == 0) {
        row_count++;
        fprintf(stdout, "[%02d]", row_count);

        fprintf(stdout, " seq:");
        if (null[0])
            fprintf(stdout, " null");
        else if (error[0])
            fprintf(stdout, " err");
        else
            fprintf(stdout, " %lli(%ld)", seq, len[0]);

        /* column 1 */
        fprintf(stdout, " from:");
        if (null[1])
            fprintf(stdout, " null");
        else if (error[1])
            fprintf(stdout, " err");
        else
            fprintf(stdout, " %lli(%ld)", from, len[1]);

        fprintf(stdout, " to:");
        if (null[2])
            fprintf(stdout, " null");
        else if (error[2])
            fprintf(stdout, " err");
        else
            fprintf(stdout, " %lli(%ld)", to, len[2]);

        /* column 3 */
        fprintf(stdout, " type:");
        if (null[3])
            fprintf(stdout, " null");
        else
            fprintf(stdout, " %i(%ld)", (int)type, len[3]);

        /* column 4 */
        fprintf(stdout, " body: ");
        if (null[4])
            fprintf(stdout, " null");
        else
            fprintf(stdout, " %s(%ld)", buf, len[4]);

        fprintf(stdout, " style:");
        if (null[5])
            fprintf(stdout, " null");
        else
            fprintf(stdout, " %i(%ld)", (int)style, len[5]);

        fprintf(stdout, " ts:");
        if (null[6])
            fprintf(stdout, " null");
        else
            fprintf(stdout, " %lli(%ld)", ts, len[6]);
        //fprintf(stdout, " %04d-%02d-%02d %02d:%02d:%02d (%ld)", ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, len[6]);
        fprintf(stdout, "\n");
    }

    if (ret != 0 && ret != MYSQL_NO_DATA) {
        if (ret == MYSQL_DATA_TRUNCATED) {
            printf("fetch error: data truncated!\n");
        }
        else {
            printf("fetch error %d errno %d info %s\n", ret, mysql_stmt_errno(stmt), mysql_stmt_error(stmt)[0]);
        }
    }


    mysql_free_result(meta_res);

    if (mysql_stmt_close(stmt)) {
        /* mysql_stmt_close() invalidates stmt, so call          */
        /* mysql_error(mysql) rather than mysql_stmt_error(stmt) */
        fprintf(stderr, " failed while closing the statement\n");
        fprintf(stderr, " %s\n", mysql_error(mysql));
        return 8;
    }

#undef FIELD_NUM
    return 0;
}

static int run_fms() {
    const int num = 1;
    for (int i = 0; i < num; ++i) {
        if (num > 1) printf("------------ %d -------------\n", i);
        int ret = fms_move(i);
        if (ret != 0) return ret;
    }

    return 0;
}

static int run_co() {
    //mysql_query(mysql, "drop table if exists test");
    //mysql_query(mysql, "create table test(id bigint,name varchar(100),val bigint,timestamp timestamp not null default now(),primary key(id))");
    mysql_query(mysql, "set names utf8");
    MYSQL_STMT* stmt = mysql_stmt_init(mysql);
    const char* sql = "select id,payer_id,receiver_id,price,state,created_time from coin_order where created_time>=? and id>=? and (wx_transaction_id!=?||wx_transaction_id is null)";
    if (mysql_stmt_prepare(stmt, sql, strlen(sql))) {
        fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
        return 1;
    }
    /* Fetch result set meta information */
    MYSQL_RES* meta_res = mysql_stmt_result_metadata(stmt);
    if (!meta_res) {
        fprintf(stderr, " mysql_stmt_result_metadata(),returned no meta information\n");
        fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
        return 2;
    }
    printf("param count: %i\n", mysql_stmt_param_count(stmt));

    /* Get total columns in the query */
    int column_num = mysql_num_fields(meta_res);
    fprintf(stdout, " total columns in SELECT statement: %d\n", column_num);

    if (column_num != 6) { /* validate column count */
        fprintf(stderr, " invalid column count returned by MySQL\n");
        return 5;
    }

    MYSQL_BIND bind[6];
    memset(&bind, 0, sizeof bind);
    MYSQL_TIME  ts;
    bind[0].buffer_type = MYSQL_TYPE_DATE;
    bind[0].buffer = (char*)&ts;
    bind[0].is_null = 0;
    bind[0].length = 0;

    long long id = 0ll;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = (char*)&id;
    bind[1].buffer_length = sizeof id;
    bind[1].is_null = 0;
    bind[1].length = 0;

    char wxid[50];
    strcpy_s(wxid, sizeof wxid, "*");
    unsigned long wxid_len = strlen(wxid);
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (char*)wxid;
    bind[2].buffer_length = wxid_len;
    bind[2].is_null = 0;
    bind[2].length = &wxid_len;

    if (mysql_stmt_bind_param(stmt, bind)) {
        fprintf(stderr, " mysql_stmt_bind_param() failed\n");
        fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
        return 1;
    }

    ts.year = 2007;
    ts.month = 07;
    ts.day = 7;
    ts.hour = 7;
    ts.minute = 7;
    ts.second = 7;
    ts.second_part = 7;
    ts.neg = false;

    //if (mysql_stmt_execute(stmt))
    if (mysql_stmt_execute(stmt)) {
        fprintf(stderr, " mysql_stmt_execute error: %s\n", mysql_stmt_error(stmt));
        return 2;
    }

    /* Get the total rows affected */
    unsigned long numrows = mysql_stmt_affected_rows(stmt);
    fprintf(stdout, " affected rows: %lu\n", numrows);

#define FIELD_NUM 6
    bool null[FIELD_NUM];
    bool error[FIELD_NUM];
    unsigned long len[FIELD_NUM];
    int payer;
    int receiver;
    char price[20];
    char state[32];

    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = (char*)&id;
    bind[0].buffer_length = sizeof id;
    bind[0].is_null = &null[0];
    bind[0].length = &len[0];
    bind[0].error = &error[0];

    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = (char*)&payer;
    bind[1].buffer_length = sizeof payer;
    bind[1].is_null = &null[1];
    bind[1].length = &len[1];
    bind[1].error = &error[1];

    bind[2].buffer_type = MYSQL_TYPE_LONG;
    bind[2].buffer = (char*)&receiver;
    bind[2].buffer_length = sizeof receiver;
    bind[2].is_null = &null[2];
    bind[2].length = &len[2];
    bind[2].error = &error[2];

    bind[3].buffer_type = MYSQL_TYPE_NEWDECIMAL;
    bind[3].buffer = (char*)price;
    bind[3].buffer_length = sizeof price;
    bind[3].is_null = &null[3];
    bind[3].length = &len[3];
    bind[3].error = &error[3];

    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = state;
    bind[4].buffer_length = sizeof state;
    bind[4].is_null = &null[4];
    bind[4].length = &len[4];
    bind[4].error = &error[4];

    bind[5].buffer_type = MYSQL_TYPE_DATETIME;
    //bind[5].buffer_type = MYSQL_TYPE_TIMESTAMP;
    bind[5].buffer = (char*)&ts;
    bind[5].is_null = &null[5];
    bind[5].length = &len[5];
    bind[5].error = &error[5];

    /* Bind the result buffers */
    if (mysql_stmt_bind_result(stmt, bind)) {
        fprintf(stderr, " mysql_stmt_bind_result() failed: %s\n", mysql_stmt_error(stmt));
        return 7;
    }

    /* Now buffer all results to client (optional step) */
    if (mysql_stmt_store_result(stmt)) {
        fprintf(stderr, " mysql_stmt_store_result() failed\n");
        fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
        return 5;
    }

    int row_count = 0;
    fprintf(stdout, "Fetching results ...\n");
    int ret = 0;
    while ((ret = mysql_stmt_fetch(stmt)) == 0) {
        row_count++;
        fprintf(stdout, "  [%02d]  ", row_count);

        fprintf(stdout, "id: ");
        if (null[0])
            fprintf(stdout, " NULL");
        else if (error[0])
            fprintf(stdout, " error");
        else
            fprintf(stdout, " %lli(%ld)", id, len[0]);

        /* column 1 */
        fprintf(stdout, "   payer: ");
        if (null[1])
            fprintf(stdout, " NULL");
        else if (error[1])
            fprintf(stdout, " error");
        else
            fprintf(stdout, " %i(%ld)", payer, len[1]);

        /* column 2 */
        fprintf(stdout, "   receiver: ");
        if (null[2])
            fprintf(stdout, " NULL");
        else
            fprintf(stdout, " %i(%ld)", receiver, len[2]);

        /* column 3 */
        fprintf(stdout, "   price: ");
        if (null[3])
            fprintf(stdout, " NULL");
        else
            fprintf(stdout, " %s(%ld)", price, len[3]);

        /* column 4 */
        fprintf(stdout, "   state: ");
        if (null[4])
            fprintf(stdout, " NULL");
        else
            fprintf(stdout, " %s(%ld)", state, len[4]);

        fprintf(stdout, "   ts: ");
        if (null[5])
            fprintf(stdout, " NULL");
        else
            fprintf(stdout, " %04d-%02d-%02d %02d:%02d:%02d (%ld)",
                ts.year, ts.month, ts.day,
                ts.hour, ts.minute, ts.second,
                len[5]);
        fprintf(stdout, "\n");
    }

    if (ret != 0 && ret != MYSQL_NO_DATA) {
        if (ret == MYSQL_DATA_TRUNCATED) {
            printf("fetch error: data truncated!\n");
        }
        else {
            printf("fetch error %d errno %d info %s\n", ret, mysql_stmt_errno(stmt), mysql_stmt_error(stmt)[0]);
        }
    }


    mysql_free_result(meta_res);

    if (mysql_stmt_close(stmt)) {
        /* mysql_stmt_close() invalidates stmt, so call          */
        /* mysql_error(mysql) rather than mysql_stmt_error(stmt) */
        fprintf(stderr, " failed while closing the statement\n");
        fprintf(stderr, " %s\n", mysql_error(mysql));
        return 3;
    }

    return 0;
}

int run() {
    return run_fms();
}

int init() {
    mysql = mysql_init(NULL);
    if (mysql == NULL) {
        return -1;
    }

    mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "binlog_rt");
    mysql_options(mysql, MYSQL_SET_CHARSET_NAME, MYSQL_AUTODETECT_CHARSET_NAME);
    int flags = CLIENT_COMPRESS | CLIENT_FOUND_ROWS;
    MYSQL* my = mysql_real_connect(mysql, MYSQL_CONN_INFO, NULL, flags);
    if (my == NULL) {
        printf("error: %s\n", mysql_error(mysql));
        mysql_close(mysql);
        mysql = NULL;
        return -1;
    }

    printf("client: %s(%i)\n", mysql_get_client_info(), mysql_get_client_version());
    printf("server: %s(%i)\n", mysql_get_server_info(mysql), mysql_get_server_version(mysql));
    return 0;
}

int finish() {
    mysql_close(mysql);
}
