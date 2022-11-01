import requests
import json
import random
import sys
from multiprocessing import Pool
from utils import  exeSQL, exeMultipleSQL, hex2wei, drop_table, create_action_table            
from tqdm import tqdm as tqdm_no_notebook
from tqdm import tqdm_notebook

def is_ijupyter():
    return 'ipykernel' in sys.modules

tqdm = tqdm_notebook if is_ijupyter() else tqdm_no_notebook
            
parity_url = 'http://127.0.0.1:8545'

def create_action_table(table_name):
     create_action_sql = """CREATE TABLE {} ( `act_id` int(11) NOT NULL AUTO_INCREMENT,
                 `directive` enum('call','create','reward-block','reward-uncle','suicide','callcode','delegatecall', 'staticcall') NOT NULL,
                 `source` char(42) NOT NULL, `target` char(42) NOT NULL, `amount` varchar(32) NOT NULL,
                 `tx` char(66) NOT NULL, `block_num` int(11) NOT NULL, `tx_seq` int(11) NOT NULL,
                 `act_seq` int(11) NOT NULL, PRIMARY KEY (`act_id`),
                 `input_data` varchar(10000) NOT NULL,
                 UNIQUE KEY `unique_action` (`block_num`,`tx_seq`,`act_seq`),KEY `block_num_index` (`block_num`),
                 FULLTEXT `target_index` (`target`), FULLTEXT `source_index`(`source`),
                 FULLTEXT `tx_index` (`tx`) ) ENGINE=InnoDB""".format(table_name)
     exeSQL(create_action_sql, True)


ex_list = [
'0x05f51aab068caa6ab7eeb672f88c180f67f17ec7',
'0x4df5f3610e2471095a130d7d934d551f3dde01ed',
'0xadb72986ead16bdbc99208086bd431c1aa38938e',
'0x7a10ec7d68a048bdae36a70e93532d31423170fa',
'0xce1bf8e51f8b39e51c6184e059786d1c0eaf360f',
'0xf73c3c65bde10bf26c2e1763104e609a41702efe',
'0xa30d8157911ef23c46c0eb71889efe6a648a41f7',
'0xf7793d27a1b76cdf14db7c83e82c772cf7c92910',
'0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',
'0xd551234ae421e3bcba99a0da6d736074f22192ff',
'0x564286362092d8e7936f0549571a803b203aaced',
'0x0681d8db095565fe8a346fa0277bffde9c0edbbf',
'0xfe9e8709d3215310075d67e3ed32a380ccf451c8',
'0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67',
'0xbe0eb53f46cd790cd13851d5eff43d12404d33e8',
'0xf977814e90da44bfa03b6295a0616a897441acec',
'0x7c49e1c0e33f3efb57d64b7690fa287c8d15b90a',
'0xdf5021a4c1401f1125cd347e394d977630e17cf7',
'0x28ebe764b8f9a853509840645216d3c2c0fd774b',
'0x1151314c646ce4e0efd76d1af4760ae66a9fe30f',
'0x7727e5113d1d161373623e5f49fd568b4f543a9e',
'0x4fdd5eb2fb260149a3903859043e962ab89d8ed4',
'0x876eabf441b2ee5b5b0554fd502a8e0600950cfa',
'0x742d35cc6634c0532925a3b844bc454e4438f44e',
'0x8fa8af91c675452200e49b4683a33ca2e1a34e42',
'0x3052cd6bf951449a984fe4b5a38b46aef9455c8e',
'0x2140efd7ba31169c69dfff6cdc66c542f0211825',
'0x3fbe1f8fc5ddb27d428aa60f661eaaab0d2000ce',
'0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1',
'0x03bdf69b1322d623836afbd27679a1c0afa067e9',
'0x4b1a99467a284cc690e3237bc69105956816f762',
'0x1522900b6dafac587d499a862861c0869be6e428',
'0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98',
'0xe94b04a0fed112f3664e45adb2b8915693dd5ff3',
'0x66f820a414680b5bcda5eeca5dea238543f42054',
'0xaa90b4aae74cee41e004bc45e45a427406c4dcae',
'0xf8d04a720520d0bcbc722b1d21ca194aa22699f2',
'0xfb9f7f41319157ac5c5dccae308a63a4337ad5d9',
'0xd7c866d0d536937bf9123e02f7c052446588189f',
'0x72bcfa6932feacd91cb2ea44b0731ed8ae04d0d3',
'0xfd648cc72f1b4e71cbdda7a0a91fe34d32abd656',
'0x96fc4553a00c117c5b0bed950dd625d1c16dc894',
'0x8958618332df62af93053cb9c535e26462c959b0',
'0xb726da4fbdc3e4dbda97bb20998cf899b0e727e0',
'0x9539e0b14021a43cde41d9d45dc34969be9c7cb0',
'0x33683b94334eebc9bd3ea85ddbda4a86fb461405',
'0xb6ba1931e4e74fd080587688f6db10e830f810d5',
'0xb9ee1e551f538a464e8f8c41e9904498505b49b0',
'0x4b01721f0244e7c5b5f63c20942850e447f5a5ee',
'0x1d1bd550197c7c0787b9ad0aea9c1cca66ee0e90',
'0x0d6b5a54f940bf3d52e438cab785981aaefdf40c',
'0xd1560b3984b7481cd9a8f40435a53c860187174d',
'0x521db06bf657ed1d6c98553a70319a8ddbac75a3',
'0x5baeac0a0417a05733884852aa068b706967e790',
'0x2984581ece53a4390d1f568673cf693139c97049',
'0xe17ee7b3c676701c66b395a35f0df4c2276a344e',
'0x915d7915f2b469bb654a7d903a5d4417cb8ea7df',
'0x4e5b2e1dc63f6b91cb6cd759936495434c7e972f',
'0x0d0707963952f2fba59dd06f2b425ace40b492fe',
'0x7793cd85c11a924478d358d49b05b37e91b5810f',
'0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c',
'0x9f5ca0012b9b72e8f3db57092a6f26bf4f13dc69',
'0xd24400ae8bfebb18ca49be86258a3c749cf46853',
'0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8',
'0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea',
'0x9fb01a2584aac5aae3fab1ed25f86c5269b32999',
'0x9c67e141c0472115aa1b98bd0088418be68fd249',
'0x59a5208b32e627891c389ebafc644145224006e8',
'0xa12431d0b9db640034b0cdfceef9cce161e62be4',
'0x274f3c32c90517975e29dfc209a23f315c1e5fc7',
'0x8533a0bd9310eb63e7cc8e1116c18a3d67b1976a',
'0xab5c66752a9e8167967685f1450532fb96d5d24f',
'0xe93381fb4c4f14bda253907b18fad305d799241a',
'0xfa4b5be3f2f84f56703c42eb22142744e95a2c58',
'0x46705dfff24256421a05d056c29e81bdc09723b8',
'0x1b93129f05cc2e840135aab154223c75097b69bf',
'0xeb6d43fe241fb2320b5a3c9be9cdfd4dd8226451',
'0x956e0dbecc0e873d34a5e39b25f364b2ca036730',
'0xeec606a66edb6f497662ea31b5eb1610da87ab5f',
'0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b',
'0xfdb16996831753d5331ff813c29a93c76834a0ad',
'0xeee28d484628d41a82d01e21d12e2e78d69920da',
'0x5c985e89dde482efe97ea9f1950ad149eb73829b',
'0xdc76cd25977e0a5ae17155770273ad58648900d3',
'0xadb2b42f6bd96f5c65920b9ac88619dce4166f94',
'0xa8660c8ffd6d578f657b72c0c811284aef0b735e',
'0x1062a747393198f70f71ec65a582423dba7e5ab3',
'0x2910543af39aba0cd09dbb2d50200b3e800a63d2',
'0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13',
'0xe853c56864a2ebe4576a807d26fdc4a0ada51919',
'0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0',
'0xfa52274dd61e1643d2205169732f29114bc240b3',
'0xe8a0e282e6a3e8023465accd47fae39dd5db010b',
'0x629a7144235259336ea2694167f3c8b856edd7dc',
'0x30b71d015f60e2f959743038ce0aaec9b4c1ea44',
'0x2b5634c42055806a59e9107ed44d43c426e58258',
'0x689c56aef474df92d44a1b70850f808488f9769c',
'0x0861fca546225fbf8806986d211c8398f7457734',
'0x7891b20c690605f4e370d6944c8a5dbfac5a451c',
'0x8271b2e8cbe29396e9563229030c89679b9470db',
'0x5e575279bf9f4acf0a130c186861454247394c06',
'0xedbb72e6b3cf66a792bff7faac5ea769fe810517',
'0x243bec9256c9a3469da22103891465b47583d9f1',
'0xe03c23519e18d64f144d2800e30e81b0065c48b5',
'0xae7006588d03bd15d6954e3084a7e644596bc251',
'0x6cc5f688a315f3dc28a7781717a9a798a59fda7b',
'0x236f9f97e0e62388479bf9e5ba4889e46b0273c3',
'0xaeec6f5aca72f3a005af1b3420ab8c8c7009bac8',
'0xbd8ef191caa1571e8ad4619ae894e07a75de0c35',
'0x2bb97b6cf6ffe53576032c11711d59bd056830ee',
'0xd4dcd2459bb78d7a645aa7e196857d421b10d93f',
'0x32be343b94f860124dc4fee278fdcbd38c102d88',
'0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef',
'0xb794f5ea0ba39494ce839613fffba74279579268',
'0xa910f92acdaf488fa6ef02174fb86208ad7722ba',
'0xaa9fa73dfe17ecaa2c89b39f0bb2779613c5fc3b',
'0x2fa2bc2ce6a4f92952921a4caa46b3727d24a1ec',
'0x31a2feb9b5d3b5f4e76c71d6c92fc46ebb3cb1c1',
'0x6b71834d65c5c4d8ed158d54b47e6ea4ff4e5437',
'0x48d466b7c0d32b61e8a82cd2bcf060f7c3f966df',
'0x0536806df512d6cdde913cf95c9886f65b1d3462',
'0x8d451ae5ee8f557a9ce7a9d7be8a8cb40002d5cb',
'0xbd2ec7c608a06fe975dbdca729e84dedb34ecc21',
'0xc0e30823e5e628df8bc9bf2636a347e1512f0ecb',
'0x65f9b2e4d7aaeb40ffea8c6f5844d5ad7da257e0',
'0x36b01066b7fa4a0fdb2968ea0256c848e9135674',
'0xab11204cfeaccffa63c2d23aef2ea9accdb0a0d5',
'0x6795cf8eb25585eadc356ae32ac6641016c550f2',
'0xfbf2173154f7625713be22e0504404ebfe021eae',
'0x6f803466bcd17f44fa18975bf7c509ba64bf3825',
'0xead6be34ce315940264519f250d8160f369fa5cd',
'0xd344539efe31f8b6de983a0cab4fb721fc69c547',
'0x5ca39c42f4dee3a5ba8fec3ad4902157d48700bf',
'0xb8cf411b956b3f9013c1d0ac8c909b086218207c',
'0x2819c144d5946404c0516b6f817a960db37d4929',
'0x13f64609bf1ef46f6515f8cd3115433a93a00dc6',
'0x120a270bbc009644e35f0bb6ab13f95b8199c4ad',
'0x9e6316f44baeeee5d41a1070516cc5fa47baf227',
'0x70faa28a6b8d6829a4b1e649d26ec9a2a39ba413',
'0x563b377a956c80d77a7c613a9343699ad6123911',
'0xd3273eba07248020bf98a8b560ec1576a612102f',
'0x3b0bc51ab9de1e5b7b6e34e5b960285805c41736',
'0xeed16856d551569d134530ee3967ec79995e2051',
'0x3613ef1125a078ef96ffc898c4ec28d73c5b8c52',
'0x0a73573cf2903d2d8305b1ecb9e9730186a312ae',
'0xb2cc3cdd53fc9a1aeaf3a68edeba2736238ddc5d',
'0x1119aaefb02bf12b84d28a5d8ea48ec3c90ef1db',
'0x2f1233ec3a4930fd95874291db7da9e90dfb2f03',
'0x390de26d772d2e2005c6d1d24afc902bae37a4bb',
'0xba826fec90cefdf6706858e5fbafcb27a290fbe0',
'0x5e032243d507c743b061ef021e2ec7fcc6d3ab89',
'0xf5bec430576ff1b82e44ddb5a1c93f6f9d0884f3',
'0xd94c9ff168dc6aebf9b6cc86deff54f3fb0afc33',
'0x42da8a05cb7ed9a43572b5ba1b8f82a0a6e263dc',
'0x700f6912e5753e91ea3fae877a2374a2db1245d7',
'0x60d0cc2ae15859f69bf74dadb8ae3bd58434976b'
] 

# def create_tmp_ex_addr_table(table_name):
    # create_action_sql = """CREATE TABLE {} ( `id` int(11) NOT NULL AUTO_INCREMENT, `address` char(42) NOT NULL,
            
                # PRIMARY KEY (`id`), 
                # KEY `address_index` (`address`) ) ENGINE=InnoDB""".format(table_name)
    # exeSQL(create_action_sql, True)
            

def findAutoTx(action_pure_ex_table):
    total_addr_set = fetchAddrsSet(action_pure_ex_table)
    slen = len(total_addr_set) // par_num
    addr_sets_list = []
    index_list = list(range(par_num))
    for i in range(par_num):
        if i < par_num-1:
            addr_sets_list.append(set(random.sample(total_addr_set, slen))) 
            total_addr_set -= addr_sets_list[i]
        else:
            addr_sets_list.append(total_addr_set)
#     print (len(addr_sets_list))
    Pool(par_num).starmap(subFindAutoTx, zip(addr_sets_list, index_list))


def subFindAutoTx(addrs_set, index):
#     actions_dict = {}
    text = "Progreeser #{}".format(index)
    for addr in tqdm(addrs_set, desc=text, position=index):
#         if addr == 'none00000000000000000000000000000000000000':
#             continue
        checkPerAddr(addr, index)

        
def checkPerAddr(addr, index):
    tmp_table_name = action_table + str(index)
    fetch_ex_addr_info_sql = ("SELECT * FROM {} WHERE source = '{}' or target = '{}' order by block_num, act_id").format( action_pure_ex_table, addr, addr)
    addr_infos = exeSQL(fetch_ex_addr_info_sql)
    #print(ex_addr[0][0])
    if len(addr_infos) == 0 or len(addr_infos) == 1:
        return
    prev_info = None
    for info in addr_infos:
        if info[2] in ex_list:
            prev_info = info
            continue
        if info[2] not in ex_list and info[3] not in ex_list:
            continue
        if prev_info == None:
            continue
        else:
            if info[3] == prev_info[2] and info[6] - prev_info[6] < 500 and int(info[4], 16) / 10 ** 18 - int(prev_info[4], 16) / 10 ** 18 < 0.1:
                insert_act_sql_1 = """INSERT INTO {} (act_id, directive, source, target, amount, tx, block_num, tx_seq, act_seq, input_data)
                    VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(tmp_table_name, prev_info[0], prev_info[1], prev_info[2], prev_info[3], prev_info[4], prev_info[5], prev_info[6], prev_info[7], prev_info[8], prev_info[9])
                insert_act_sql_2 = """INSERT INTO {} (act_id, directive, source, target, amount, tx, block_num, tx_seq, act_seq, input_data)
                    VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(tmp_table_name, info[0], info[1], info[2], info[3], info[4], info[5], info[6], info[7], info[8], info[9])
                insert_sql = []
                insert_sql.append(insert_act_sql_1)
                insert_sql.append(insert_act_sql_2)
                exeMultipleSQL(insert_sql, True)
                print(info)
                prev_info = None
                
    # # fetch_user_info_sql = ("SELECT * FROM {} WHERE target = '{}' ").format( action_table_name, addr)
    # # user_info = exeSQL(fetch_user_info_sql)
    # # tmp_table_name = 'action_tmp_ex_20170901_20170930' + str(index)
    # # insert_act_sql = """INSERT INTO {} (directive, source, target, amount, tx, block_num, tx_seq, act_seq)
             # # VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(tmp_table_name, user_info[0][1], user_info[0][2], ex_addr[0][0], user_info[0][4], user_info[0][5], user_info[0][6], user_info[0][7], user_info[0][8])
    # # exeSQL(insert_sql, True)
    #fetch_ex_addr_info_sql = ("UPDATE {} SET target = '{}' WHERE target = '{}' ").format(action_table_name, ex_addr[0][0], addr)
    #exeSQL(fetch_ex_addr_info_sql, True)
    #fetch_ex_addr_info_sql = ("DELETE  WHERE source = '{}' ").format(action_table_name, addr)
    #exeSQL(fetch_ex_addr_info_sql, True)
    # if count_send[0][0] == 1 and count_recv[0][0] == 1:
        # fetch_ex_addr_info_sql = ("SELECT block_num, amount FROM {} WHERE source = '{}' ").format( action_table_name, addr)
        # info_send = exeSQL(fetch_ex_addr_info_sql)
        # fetch_ex_addr_info_sql = ("SELECT block_num, amount FROM {} WHERE target = '{}' ").format( action_table_name, addr)
        # info_recv = exeSQL(fetch_ex_addr_info_sql)
        # print(info_send)
        # print(info_recv)
        # os._exit()
        #if info_send[0][0] == 1 and count_recv[0][0] == 1:
        #insert_sql =  """INSERT INTO {} (address)
        #    VALUES ('{}')""".format(ex_table_name, addr)
        #exeSQL(insert_sql, True)

    
    
def fetchAddrsSet(action_pure_ex_table):
     fetch_addrs_sql = ("SELECT {} FROM {}").format("source", action_pure_ex_table)
     addrs = exeSQL(fetch_addrs_sql)
     flat_addrs = [item for sublist in addrs for item in sublist]
     addr_set = set((flat_addrs))
     addr_set -= set(ex_list)
     #print(ex_addr_set)
     #print("/n")
     #print(len(ex_addr_set))
     return (addr_set)
    
action_table = 'action_auto_same_ex_20170901_20170930_'
action_pure_ex_table = 'action_pure_ex_20170901_20170930'
par_num = 50

for i in range(par_num):
    table_name = action_table + str(i)
    create_action_table(table_name)
    
findAutoTx(action_pure_ex_table)

