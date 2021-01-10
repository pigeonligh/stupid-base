package main

var configs = map[string][]string{
	/*
		"customer": {
			"c_custkey       int(8) not null",
			"c_name          varchar(32)",
			"c_address       varchar(64)",
			"c_nationkey     int(8) not null",
			"c_phone         varchar(32)",
			"c_acctbal       float not null",
			"c_mktsegment    varchar(32)",
			"c_comment       varchar(200)",
		},
		"nation": {
			"n_nationkey    int(8) not null",
			"n_name         varchar(32)",
			"n_regionkey    int(8) not null",
			"n_comment      varchar(200)",
		},
		"orders": {
			"o_orderkey         int(8) not null",
			"o_custkey          int(8) not null",
			"o_orderstatus      varchar(32)",
			"o_totalprice       float not null",
			"o_orderdate        date not null",
			"o_orderpriority    varchar(32)",
			"o_clerk            varchar(32)",
			"o_shippriority     int(8) not null",
			"o_comment          varchar(200)",
		},
		"part": {
			"p_partkey        int(8) not null",
			"p_name           varchar(64)",
			"p_mfgr           varchar(32)",
			"p_brand          varchar(32)",
			"p_type           varchar(32)",
			"p_size           int(8) not null",
			"p_container      varchar(32)",
			"p_retailprice    int(8) not null",
			"p_comment        varchar(32)",
		},
		"partsupp": {
			"ps_partkey       int(8) not null",
			"ps_suppkey       int(8) not null",
			"ps_availqty      int(8) not null",
			"ps_supplycost    float not null",
			"ps_comment       varchar(200)",
		},
	*/
	"region": {
		"r_regionkey    int(8) not null",
		"r_name         varchar(32)",
		"r_comment      varchar(200)",
	},
}
