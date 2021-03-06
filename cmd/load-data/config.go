package main

var configs = map[string][]string{
	"customer": {
		"C_CUSTKEY			INT NOT NULL,",
		"C_NAME				VARCHAR(25),",
		"C_ADDRESS			VARCHAR(40),",
		"C_NATIONKEY		INT NOT NULL, ",
		"C_PHONE			VARCHAR(15),",
		"C_ACCTBAL			FLOAT,",
		"C_MKTSEGMENT		VARCHAR(10),",
		"C_COMMENT			VARCHAR(117),",
		"PRIMARY KEY 		(C_CUSTKEY)",
	},
	"nation": {
		"N_NATIONKEY		INT NOT NULL,",
		"N_NAME				VARCHAR(25),",
		"N_REGIONKEY		INT NOT NULL,",
		"N_COMMENT			VARCHAR(152),",
		"PRIMARY KEY		(N_NATIONKEY)",
	},
	"orders": {
		"O_ORDERKEY			INT NOT NULL,",
		"O_CUSTKEY			INT NOT NULL,",
		"O_ORDERSTATUS		VARCHAR(1),",
		"O_TOTALPRICE		FLOAT,",
		"O_ORDERDATE		DATE,",
		"O_ORDERPRIORITY	VARCHAR(15),",
		"O_CLERK			VARCHAR(15),",
		"O_SHIPPRIORITY		INT,",
		"O_COMMENT			VARCHAR(79),",
		"PRIMARY KEY		(O_ORDERKEY)",
	},
	"part": {
		"P_PARTKEY			INT NOT NULL,",
		"P_NAME				VARCHAR(55),",
		"P_MFGR				VARCHAR(25),",
		"P_BRAND			VARCHAR(10),",
		"P_TYPE				VARCHAR(25),",
		"P_SIZE				INT,",
		"P_CONTAINER		VARCHAR(10),",
		"P_RETAILPRICE		FLOAT,",
		"P_COMMENT			VARCHAR(23),",
		"PRIMARY KEY    	(P_PARTKEY)",
	},
	"partsupp": {
		"PS_PARTKEY			INT NOT NULL, ",
		"PS_SUPPKEY			INT NOT NULL,",
		"PS_AVAILQTY		INT,",
		"PS_SUPPLYCOST		FLOAT,",
		"PS_COMMENT			VARCHAR(199),",
		"PRIMARY KEY 		(PS_PARTKEY, PS_SUPPKEY)",
	},
	"region": {
		"R_REGIONKEY		INT NOT NULL,",
		"R_NAME				VARCHAR(25),",
		"R_COMMENT			VARCHAR(152),",
		"PRIMARY KEY 		(R_REGIONKEY)",
	},
	"lineitem": {
		"L_ORDERKEY			INT NOT NULL, ",
		"L_PARTKEY			INT NOT NULL, ",
		"L_SUPPKEY			INT NOT NULL, ",
		"L_LINENUMBER		INT,",
		"L_QUANTITY			FLOAT,",
		"L_EXTENDEDPRICE	FLOAT,",
		"L_DISCOUNT			FLOAT,",
		"L_TAX				FLOAT,",
		"L_RETURNFLAG		VARCHAR(1),",
		"L_LINESTATUS		VARCHAR(1),",
		"L_SHIPDATE			DATE,",
		"L_COMMITDATE		DATE,",
		"L_RECEIPTDATE		DATE,",
		"L_SHIPINSTRUCT		VARCHAR(25),",
		"L_SHIPMODE			VARCHAR(10),",
		"L_COMMENT			VARCHAR(44),",
		"PRIMARY KEY 		(L_ORDERKEY, L_LINENUMBER)",
	},
	"supplier": {
		"S_SUPPKEY			INT NOT NULL,",
		"S_NAME				VARCHAR(25),",
		"S_ADDRESS			VARCHAR(40),",
		"S_NATIONKEY		INT NOT NULL, ",
		"S_PHONE			VARCHAR(15),",
		"S_ACCTBAL			FLOAT,",
		"S_COMMENT			VARCHAR(101),",
		"PRIMARY KEY    	(S_SUPPKEY)",
	},
}

var configOrder = []string{
	"part",
	"region",
	"nation",
	"supplier",
	"customer",
	"partsupp",
	"orders",
	"lineitem",
}

var afterCommand = []string{
	"ALTER TABLE SUPPLIER ADD CONSTRAINT fk1 FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY)",
	"ALTER TABLE PARTSUPP ADD CONSTRAINT fk2 FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY);",
	"ALTER TABLE PARTSUPP ADD CONSTRAINT fk3 FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY);",
	"ALTER TABLE CUSTOMER ADD CONSTRAINT fk4 FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY);",
	"ALTER TABLE ORDERS ADD CONSTRAINT fk5 FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY);",
	"ALTER TABLE LINEITEM ADD CONSTRAINT fk6 FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY);",
	"ALTER TABLE LINEITEM ADD CONSTRAINT fk7 FOREIGN KEY (L_PARTKEY,L_SUPPKEY) REFERENCES PARTSUPP(PS_PARTKEY,PS_SUPPKEY);",
	"ALTER TABLE NATION ADD CONSTRAINT fk8 FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY);",
}
