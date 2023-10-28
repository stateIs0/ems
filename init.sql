create table ems.QRTZ_CALENDARS
(
    SCHED_NAME    varchar(120) not null,
    CALENDAR_NAME varchar(200) not null,
    CALENDAR      blob         not null,
    primary key (SCHED_NAME, CALENDAR_NAME)
);

create table ems.QRTZ_FIRED_TRIGGERS
(
    SCHED_NAME        varchar(120) not null,
    ENTRY_ID          varchar(95)  not null,
    TRIGGER_NAME      varchar(200) not null,
    TRIGGER_GROUP     varchar(200) not null,
    INSTANCE_NAME     varchar(200) not null,
    FIRED_TIME        bigint       not null,
    SCHED_TIME        bigint       not null,
    PRIORITY          int          not null,
    STATE             varchar(16)  not null,
    JOB_NAME          varchar(200) null,
    JOB_GROUP         varchar(200) null,
    IS_NONCONCURRENT  varchar(1)   null,
    REQUESTS_RECOVERY varchar(1)   null,
    primary key (SCHED_NAME, ENTRY_ID)
);

create table ems.QRTZ_JOB_DETAILS
(
    SCHED_NAME        varchar(120) not null,
    JOB_NAME          varchar(200) not null,
    JOB_GROUP         varchar(200) not null,
    DESCRIPTION       varchar(250) null,
    JOB_CLASS_NAME    varchar(250) not null,
    IS_DURABLE        varchar(1)   not null,
    IS_NONCONCURRENT  varchar(1)   not null,
    IS_UPDATE_DATA    varchar(1)   not null,
    REQUESTS_RECOVERY varchar(1)   not null,
    JOB_DATA          blob         null,
    primary key (SCHED_NAME, JOB_NAME, JOB_GROUP)
);

create table ems.QRTZ_LOCKS
(
    SCHED_NAME varchar(120) not null,
    LOCK_NAME  varchar(40)  not null,
    primary key (SCHED_NAME, LOCK_NAME)
);

create table ems.QRTZ_PAUSED_TRIGGER_GRPS
(
    SCHED_NAME    varchar(120) not null,
    TRIGGER_GROUP varchar(200) not null,
    primary key (SCHED_NAME, TRIGGER_GROUP)
);

create table ems.QRTZ_SCHEDULER_STATE
(
    SCHED_NAME        varchar(120) not null,
    INSTANCE_NAME     varchar(200) not null,
    LAST_CHECKIN_TIME bigint       not null,
    CHECKIN_INTERVAL  bigint       not null,
    primary key (SCHED_NAME, INSTANCE_NAME)
);

create table ems.QRTZ_TRIGGERS
(
    SCHED_NAME     varchar(120) not null,
    TRIGGER_NAME   varchar(200) not null,
    TRIGGER_GROUP  varchar(200) not null,
    JOB_NAME       varchar(200) not null,
    JOB_GROUP      varchar(200) not null,
    DESCRIPTION    varchar(250) null,
    NEXT_FIRE_TIME bigint       null,
    PREV_FIRE_TIME bigint       null,
    PRIORITY       int          null,
    TRIGGER_STATE  varchar(16)  not null,
    TRIGGER_TYPE   varchar(8)   not null,
    START_TIME     bigint       not null,
    END_TIME       bigint       null,
    CALENDAR_NAME  varchar(200) null,
    MISFIRE_INSTR  smallint     null,
    JOB_DATA       blob         null,
    primary key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    constraint QRTZ_TRIGGERS_ibfk_1
        foreign key (SCHED_NAME, JOB_NAME, JOB_GROUP) references ems.QRTZ_JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP)
);

create table ems.QRTZ_BLOB_TRIGGERS
(
    SCHED_NAME    varchar(120) not null,
    TRIGGER_NAME  varchar(200) not null,
    TRIGGER_GROUP varchar(200) not null,
    BLOB_DATA     blob         null,
    primary key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    constraint QRTZ_BLOB_TRIGGERS_ibfk_1
        foreign key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP) references ems.QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
);

create table ems.QRTZ_CRON_TRIGGERS
(
    SCHED_NAME      varchar(120) not null,
    TRIGGER_NAME    varchar(200) not null,
    TRIGGER_GROUP   varchar(200) not null,
    CRON_EXPRESSION varchar(200) not null,
    TIME_ZONE_ID    varchar(80)  null,
    primary key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    constraint QRTZ_CRON_TRIGGERS_ibfk_1
        foreign key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP) references ems.QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
);

create table ems.QRTZ_SIMPLE_TRIGGERS
(
    SCHED_NAME      varchar(120) not null,
    TRIGGER_NAME    varchar(200) not null,
    TRIGGER_GROUP   varchar(200) not null,
    REPEAT_COUNT    bigint       not null,
    REPEAT_INTERVAL bigint       not null,
    TIMES_TRIGGERED bigint       not null,
    primary key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    constraint QRTZ_SIMPLE_TRIGGERS_ibfk_1
        foreign key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP) references ems.QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
);

create table ems.QRTZ_SIMPROP_TRIGGERS
(
    SCHED_NAME    varchar(120)   not null,
    TRIGGER_NAME  varchar(200)   not null,
    TRIGGER_GROUP varchar(200)   not null,
    STR_PROP_1    varchar(512)   null,
    STR_PROP_2    varchar(512)   null,
    STR_PROP_3    varchar(512)   null,
    INT_PROP_1    int            null,
    INT_PROP_2    int            null,
    LONG_PROP_1   bigint         null,
    LONG_PROP_2   bigint         null,
    DEC_PROP_1    decimal(13, 4) null,
    DEC_PROP_2    decimal(13, 4) null,
    BOOL_PROP_1   varchar(1)     null,
    BOOL_PROP_2   varchar(1)     null,
    primary key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP),
    constraint QRTZ_SIMPROP_TRIGGERS_ibfk_1
        foreign key (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP) references ems.QRTZ_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP)
);

create index SCHED_NAME
    on ems.QRTZ_TRIGGERS (SCHED_NAME, JOB_NAME, JOB_GROUP);

create table ems.ems_dead_msg
(
    id             bigint auto_increment,
    topic_name     varchar(255)                       not null,
    group_name     varchar(255)                       not null,
    offset         bigint                             not null,
    consumer_times int      default 1                 not null,
    create_time    datetime default CURRENT_TIMESTAMP not null,
    update_time    datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    deleted        tinyint  default 0                 not null,
    primary key (id, topic_name, group_name)
);

create table ems.ems_group_client_table
(
    id                 bigint auto_increment,
    group_name         varchar(255)                       not null,
    client_id          varchar(255)                       not null,
    renew_time         datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    state              int                                not null,
    last_offset        bigint                             null,
    last_consumer_time datetime default CURRENT_TIMESTAMP null,
    create_time        datetime default CURRENT_TIMESTAMP not null,
    update_time        datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    deleted            tinyint  default 0                 not null,
    primary key (id, group_name)
);

create table ems.ems_retry_msg
(
    id                 bigint auto_increment
        primary key,
    old_topic_name     varchar(255)                       not null,
    retry_topic_name   varchar(255)                       null,
    group_name         varchar(255)                       not null,
    offset             bigint                             not null,
    consumer_times     int      default 1                 not null,
    next_consumer_time datetime default (now())           not null,
    client_id          varchar(255)                       not null,
    state              int      default 1                 not null comment '1. 重试中, 2. 重试成功;',
    create_time        datetime default CURRENT_TIMESTAMP not null,
    update_time        datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    deleted            tinyint  default 0                 not null
);

create index ems_retry_msg_client_id_index
    on ems.ems_retry_msg (client_id);

create index ems_retry_msg_consumer_times_index
    on ems.ems_retry_msg (consumer_times);

create index ems_retry_msg_group_name_index
    on ems.ems_retry_msg (group_name);

create index ems_retry_msg_id_index
    on ems.ems_retry_msg (id);

create index ems_retry_msg_next_consumer_time_index
    on ems.ems_retry_msg (next_consumer_time);

create index ems_retry_msg_retry_topic_name_index
    on ems.ems_retry_msg (retry_topic_name);

create table ems.ems_simple_group
(
    id          bigint auto_increment,
    topic_name  varchar(255)                           not null,
    group_type  varchar(255) default 'CLUSTER'         null,
    group_name  varchar(255)                           not null,
    create_time datetime     default CURRENT_TIMESTAMP not null,
    update_time datetime     default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    deleted     tinyint      default 0                 not null,
    primary key (id, topic_name, group_name),
    constraint u_id
        unique (topic_name, group_name)
);

create table ems.ems_simple_msg
(
    id             bigint auto_increment,
    topic_name     varchar(255)                       not null,
    physics_offset bigint                             not null,
    json_body      text                               null,
    from_ip        varchar(255)                       not null,
    properties     text                               null,
    tags           varchar(255)                       null,
    create_time    datetime default CURRENT_TIMESTAMP not null,
    update_time    datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    deleted        tinyint  default 0                 not null,
    primary key (id, topic_name, physics_offset),
    constraint simple_msg_pk
        unique (topic_name, physics_offset)
);

create index create_time_index
    on ems.ems_simple_msg (create_time);

create table ems.ems_simple_stream_system_config
(
    id           bigint auto_increment,
    simple_key   varchar(255)                       not null,
    simple_value varchar(255)                       not null,
    create_time  datetime default CURRENT_TIMESTAMP not null,
    update_time  datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    deleted      tinyint  default 0                 not null,
    primary key (id, simple_key)
);

create table ems.ems_simple_topic
(
    id          bigint auto_increment,
    topic_name  varchar(255)                       not null,
    rule        int      default 1                 not null comment '1 可读可写，2 写，3 读，4 禁止读写',
    type        int      default 1                 not null,
    create_time datetime default CURRENT_TIMESTAMP not null,
    update_time datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    deleted     tinyint  default 0                 not null,
    primary key (id, topic_name),
    constraint simple_topic_pk
        unique (topic_name)
);

create table ems.ems_topic_group_log
(
    id             bigint auto_increment
        primary key,
    topic_name     varchar(255)                       not null,
    physics_offset bigint                             not null,
    group_name     varchar(255)                       not null,
    client_id      varchar(255)                       not null,
    state          int                                null,
    error_msg      text                               null,
    create_time    datetime default CURRENT_TIMESTAMP not null,
    update_time    datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    deleted        tinyint  default 0                 not null
);

create index ems_topic_group_log_id_client_id_index
    on ems.ems_topic_group_log (id, client_id);

create index ems_topic_group_log_id_state_index
    on ems.ems_topic_group_log (id, state);

create index ems_topic_group_log_state_index
    on ems.ems_topic_group_log (state);

create index topic_group_log__index_client_id
    on ems.ems_topic_group_log (client_id);

create index topic_group_log__index_group_name
    on ems.ems_topic_group_log (group_name);

create index topic_group_log__index_topic_name
    on ems.ems_topic_group_log (topic_name);

create index topic_group_log_id_index_physics_offset
    on ems.ems_topic_group_log (physics_offset);

create table ems.running_log
(
    id             bigint auto_increment,
    topic_name     varchar(255)                           not null,
    group_name     varchar(255)                           not null,
    client_id      varchar(255) default ''                not null,
    consumer_times int          default (now())           null,
    create_time    datetime     default CURRENT_TIMESTAMP not null,
    offset         varchar(255)                           null,
    primary key (id, topic_name, group_name),
    constraint running_log_pk
        unique (topic_name, group_name, offset)
);

create table ems.running_log_bak
(
    id             bigint   default 0                 not null,
    topic_name     varchar(255)                       not null,
    group_name     varchar(255)                       not null,
    client_id      varchar(255)                       not null,
    consumer_times int                                null,
    create_time    datetime default CURRENT_TIMESTAMP not null,
    offset         varchar(255)                       null
);

