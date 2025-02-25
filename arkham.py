import asyncio
import json
import time
import logging
import os
import argparse
import requests
import redis
import pandas as pd
from tqdm import tqdm
from playwright.async_api import async_playwright, Playwright, Response


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s %(processName)s %(process)d %(thread)d %(module)s %(filename)s %(funcName)s %(lineno)d %(levelname)s : %(message)s',)

CFG_REDIS = {
    'host': 'xxxxx',
    'port': "xxx",
    'db': "xxx"
}


class Arkham:

    def __init__(self):
        self.url_base = 'https://intel.arkm.com/explorer/entity/'
        self.page = None
        self.redis_conn = redis.StrictRedis(host=CFG_REDIS['host'], port=CFG_REDIS['port'], db=CFG_REDIS['db'])
        self.output_file = 'transfers.json'

    async def on_response(self, response: Response):
        if 'transfer' in response.url:
            if response and response.status == 200:
                data = await response.json()
                self.parse_transfer_result(data)

    # 请求拦截器函数，设置拦截条件并可作修改
    async def intercept_request(self, interceptedRequest, page):
        if 'transfer' in interceptedRequest.url:
            headers = interceptedRequest.headers
            if 'x-payload' in headers:
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'accept-encoding': 'gzip, deflate, zstd',
                    'accept-language': 'zh-CN,zh;q=0.9',
                    'origin': 'https://platform.arkhamintelligence.com',
                    'priority': 'u=1, i',
                    'referer': 'https://platform.arkhamintelligence.com/',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", '
                                 '"Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                  '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                    'x-payload': headers['x-payload'],
                    'x-timestamp': headers['x-timestamp']
                }
                url = interceptedRequest.url
                resp = requests.get(url.replace('limit=16', 'limit=500'), headers=headers)
                # time.sleep(2)
                await page.wait_for_timeout(3000)
                if resp and resp.status_code == 200:
                    self.parse_transfer_result(resp.json())

                else:
                    await page.wait_for_timeout(3000)
                    logging.error(f'get transfer failed, url: {url}, resp: {resp}, content: {resp.text}')

    async def intercept_response(self, response):
        url = response.url
        if 'transfer' in url:
            if response.status == 200:
                # import pdb; pdb.set_trace()
                resp_json = await response.json()
                self.parse_transfer_result(resp_json)


    async def intercept_finished(self, resp):
        pass
        # await resp.text()

    def write_to_file(self, data):
        """
        将数据写入文件，以 JSON 格式保存。
        """
        try:
            with open(self.output_file, 'a', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False)
                file.write('\n')  # 每条记录占一行
        except IOError as e:
            logging.error(f"写入文件时发生错误: {e}")    
            
    def parse_transfer_result(self, resp_json):
        transfers = resp_json.get('transfers', [])
        logging.info(f'获取到 {len(transfers)} 条转账记录')
        for idx in tqdm(range(len(transfers))):
            t = transfers[idx]
            chain = (
                t.get('chain', '') or
                t.get('fromAddress', {}).get('chain', '') or
                t.get('toAddress', {}).get('chain', '') or
                t.get('fromAddresses', [{}])[0].get('address', {}).get('chain', '') or
                t.get('toAddresses', [{}])[0].get('address', {}).get('chain', '')
            )
            key_prefix = '' if chain == 'ethereum' else f"{chain}:"

            if chain == 'ethereum':
                # 检查并处理 fromAddresses
                if t.get('fromAddresses', []):
                    for fa in t['fromAddresses']:
                        address_info = fa.get('address', {})
                        if 'arkhamLabel' in address_info or 'arkhamEntity' in address_info:
                            self.write_to_file({
                                'key': f"{key_prefix}{address_info.get('address', '').lower()}",
                                'data': address_info
                            })

                # 检查并处理 toAddresses
                if t.get('toAddresses', []):
                    for ta in t['toAddresses']:
                        address_info = ta.get('address', {})
                        arkham_entity = address_info.get('arkhamEntity', {})
                        if arkham_entity.get('name') == 'Chainflip':
                            self.write_to_file({
                                'key': f"{key_prefix}{address_info.get('address', '').lower()}",
                                'data': address_info
                            })

                # 检查并处理 fromAddress
                from_address_info = t.get('fromAddress', {})
                if 'arkhamLabel' in from_address_info or 'arkhamEntity' in from_address_info:
                    self.write_to_file({
                        'key': f"{key_prefix}{from_address_info.get('address', '').lower()}",
                        'data': from_address_info
                    })

                # 检查并处理 toAddress
                to_address_info = t.get('toAddress', {})
                arkham_entity = to_address_info.get('arkhamEntity', {})
                if arkham_entity.get('name') == 'Chainflip':
                    self.write_to_file({
                        'key': f"{key_prefix}{to_address_info.get('address', '').lower()}",
                        'data': to_address_info
                    })
                # # 处理 fromAddresses
                # if t.get('fromAddresses', []):
                #     for fa in t['fromAddresses']:
                #         address_info = fa.get('address', {})
                #         if 'arkhamLabel' in address_info or 'arkhamEntity' in address_info:
                #             self.write_to_file({
                #                 'key': f"{key_prefix}{address_info['address'].lower()}",
                #                 'data': address_info
                #             })

                # # 处理 toAddresses
                # if t.get('toAddresses', []):
                #     for ta in t['toAddresses']:
                #         address_info = ta.get('address', {})
                #         if 'arkhamLabel' in address_info or 'arkhamEntity' in address_info:
                #             self.write_to_file({
                #                 'key': f"{key_prefix}{address_info['address'].lower()}",
                #                 'data': address_info
                #             })

                # # 处理 fromAddress
                # from_address_info = t.get('fromAddress', {})
                # if 'arkhamLabel' in from_address_info or 'arkhamEntity' in from_address_info:
                #     self.write_to_file({
                #         'key': f"{key_prefix}{from_address_info['address'].lower()}",
                #         'data': from_address_info
                #     })

                # # 处理 toAddress
                # to_address_info = t.get('toAddress', {})
                # arkham_entity = to_address_info.get('arkhamEntity', {})
                # if 'arkhamLabel' in to_address_info or 'arkhamEntity' in to_address_info:
                #     self.write_to_file({
                #         'key': f"{key_prefix}{to_address_info['address'].lower()}",
                #         'data': to_address_info
                #     })


    # def parse_transfer_result(self, resp_json):
    #     transfers = resp_json['transfers']
    #     logging.info(f'get {len(transfers)} transfers')
    #     for idx in tqdm(range(len(transfers))):
    #         t = transfers[idx]
    #         chain = t.get('chain', '') or t.get('fromAddress', {}).get('chain', '') or t.get('toAddress', {}).get('chain', '') or t.get('fromAddresses', [{}])[0].get('address', {}).get('chain', '') or \
    #                 t.get('toAddresses', [{}])[0].get('address', {}).get('chain', '')
    #         key_prefix = '' if chain == 'ethereum' else f"{chain}:"

    #         if t.get('fromAddresses', []):
    #             for fa in t['fromAddresses']:
    #                 if 'arkhamLabel' in fa.get('address', {}) or 'arkhamEntity' in fa.get('address', {}):
    #                     self.redis_conn.set(f"{key_prefix}{fa['address']['address'].lower()}", json.dumps(fa['address']))

    #         if t.get('toAddresses', []):
    #             for ta in t['toAddresses']:
    #                 if 'arkhamLabel' in ta.get('address', {}) or 'arkhamEntity' in ta.get('address', {}):
    #                     self.redis_conn.set(f"{key_prefix}{ta['address']['address'].lower()}", json.dumps(ta['address']))

    #         if 'arkhamLabel' in t.get('fromAddress', {}) or 'arkhamEntity' in t.get('fromAddress', {}):
    #             self.redis_conn.set(f"{key_prefix}{t['fromAddress']['address'].lower()}", json.dumps(t['fromAddress']))
    #         if 'arkhamLabel' in t.get('toAddress', {}) or 'arkhamEntity' in t.get('toAddress', {}):
    #             self.redis_conn.set(f"{key_prefix}{t['toAddress']['address'].lower()}", json.dumps(t['toAddress']))

    async def load_page(self, file=''):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, slow_mo=5000)
            ctx = await browser.new_context()
            page = await ctx.new_page()
            # page.on('response', lambda response: asyncio.ensure_future(self.on_response(response)))
            logging.info(f"file: {file}")
            page.on('request', lambda req: asyncio.ensure_future(self.intercept_request(req, page)))
            # await asyncio.wait([page.goto('https://intel.arkm.com/', options={"waitUntil": 'networkidle2', "timeout": 60 * 1000}), page.waitForNavigation()])
            # await page.goto('https://intel.arkm.com/', options={'waitUntil': 'networkidle2'})
            await page.goto('https://intel.arkm.com/')
            # await page.wait_for_load_state('networkidle')
            await page.wait_for_timeout(8000)
            # time.sleep(3)
            # import pdb; pdb.set_trace()
            if file:
                with open(file, 'r') as f:
                    lines_list = f.readlines()
                    for idx in tqdm(range(len(lines_list))):
                        addr = lines_list[idx].strip()
                        url = f"{self.url_base}{addr}"
                        # if os.path.exists(f'addresses/{addr}') and os.path.exists(f'contracts/{addr}'):
                        #     logging.info(f"skip {addr}")
                        #     continue
                        logging.info(f"start {url}...")
                        await page.goto(url)
                        # time.sleep(3)
                        await page.wait_for_timeout(8000)
                        # await page.wait_for_load_state('networkidle')

            await page.close()
            # self.page = None
            await browser.close()

    def run(self, file='address.list'):
        asyncio.run(self.load_page(file))

        # asyncio.get_event_loop().run_until_complete(self.load_page())


def json2excel(data, name):
    df = pd.DataFrame(data)
    df.to_excel(f"{name}.xlsx", index=False)


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def read_json_file(file):
    if not os.path.exists(file):
        return {}
    with open(file, 'r') as f:
        data = json.load(f)
    return data


def read_address_data(addr):
    file = f"addresses/{addr}"
    return read_json_file(file)


def read_contract_data(addr):
    file = f"contracts/{addr}"
    return read_json_file(file)


def read_dex_aggr_file(file):
    dex_aggr_data = read_json_file(file)
    addresses = dex_aggr_data["addresses"]
    data = []
    for idx in tqdm(range(len(addresses))):
        addr_info = addresses[idx]
        addr = addr_info["address"]
        ret = merge_address_contract_by_addr(addr)
        if ret:
            data.append(ret)
    return data


def merge_address_contract_by_addr(addr):
    addr_data = read_address_data(addr)
    contract_data = read_contract_data(addr)

    if not addr_data:
        logging.info(f"address {addr} not found")
        return
    if not contract_data:
        logging.info(f"contract {addr} not found")
        return

    if not addr_data.get('arkhamEntity') and not addr_data.get('arkhamLabel'):
        for key, value in addr_data.items():
            try:
                if value.get('contract', ''):
                    addr_data = value
            except Exception as e:
                import pdb; pdb.set_trace()
                print()

    arkhamEntity = addr_data.get('arkhamEntity', {})
    arkhamLabel = addr_data.get('arkhamLabel', {})

    chain = contract_data.get('deployer', {}).get('chain')
    deployer_addr = contract_data.get('deployer', {}).get('address', '')
    internel_addr = contract_data.get('internalDeployer', {}).get('address', '') if contract_data.get('internalDeployer', {}) else ''

    return {
        "Draft Labels(All)": addr,
        "blockTimestamp": contract_data['blockTimestamp'],
        "Chain": chain,
        "Txn_Hash": contract_data['transactionHash'],
        "Label": arkhamLabel.get('name', ''),
        "Entity": arkhamEntity.get('name', ''),
        "Deployer": contract_data['deployer']['address'],
        "Deployer_Label": contract_data['deployer'].get('arkhamLabel', {}).get('name', ''),
        "Deployer_Entity": contract_data['deployer'].get('arkhamEntity', {}).get('name', ''),
        "Factory": internel_addr if internel_addr != deployer_addr else '',
        "Factory_Entity": contract_data.get('internalDeployer', {}).get('arkhamEntity', {}).get('name', '') if internel_addr and internel_addr != deployer_addr else '',
        "Factory_Label": contract_data.get('internalDeployer', {}).get('arkhamLabel', {}).get('name', '') if internel_addr and internel_addr != deployer_addr else ''
    }


def read_addr_list_file(file):
    addresses = []
    with open(file, 'r') as f:
        for lin in f:
            data = merge_address_contract_by_addr(lin.strip())
            if data:
                addresses.append(data)
    return addresses


def save_file_with_json(path, file, data):
    if not os.path.exists(path):
        os.makedirs(path)
    with open(f"{path}/{file}", 'w') as f:
        f.write(json.dumps(data))


def save_address_label(req):
    if os.path.exists(f"addresses/{req['address']}"):
        return

    logging.info(f"requesting type address {req['address']}")
    resp = requests.get(url=req['url'], headers=req['headers'])
    if resp.status_code == 200:
        data = resp.json()
        # for chain, value in data.items():
        #     if value and value['contract']:
        #         save_contract_label({
        #             'url': f"https://api.arkhamintelligence.com/intelligence/contract/{chain}/{req['address']}",
        #             'address': req['address'],
        #             'headers': req['headers'],
        #             'chain': chain,
        #             'type': 'contract'
        #         })
        # TODO add chain?
        # chain = data['chain']
        # if not os.path.exists(f"addresses/{chain}"):
        #     os.makedirs(f"addresses/{chain}")
        save_file_with_json('addresses', req['address'], data)

def save_contract_label(req):
    if os.path.exists(f"contracts/{req['address']}"):
        return
    logging.info(f"requesting type contract {req['address']}")
    resp = requests.get(url=req['url'], headers=req['headers'])
    # import pdb; pdb.set_trace()
    if resp.status_code == 200:
        save_file_with_json('contracts', req['address'], resp.json())


# 消费队列
def spend_quenue():
    r = redis.Redis(**CFG_REDIS)
    while True:
        if r.llen('arkham_quenue') > 0:
            addr_info = r.rpop('arkham_quenue')
            request_info = json.loads(addr_info)
            if request_info.get('type', '') == 'address':
                save_address_label(request_info)
            elif request_info.get('type', '') == 'contract':
                save_contract_label(request_info)
            time.sleep(1)


def make_excel_by_all_addr():
    addr_list = os.listdir('addresses')
    ret_list = []
    for addr in addr_list:
        ret = merge_address_contract_by_addr(addr)
        if ret:
            ret_list.append(ret)
    with open('all_address.json', 'w') as f:
        f.write(json.dumps(ret_list))
    return ret_list

# 生成excel文件


def make_excel(file_name='address_info', make_all=False, from_file=None):
    all_data = []
    if make_all:
        all_data += make_excel_by_all_addr()
    else:
        # for i in range(1, 11):
        #     file = f"dex-aggregator-{i}.json"
        #     logging.info(f"read file {file}")
        #     data = read_dex_aggr_file(file)
        #     all_data += data
        with open('address.list', 'r') as f:
            for lin in f:
                data = merge_address_contract_by_addr(lin.strip())
                if data:
                    all_data.append(data)
    # TODO other file
    json2excel(all_data, file_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='start arkham', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s', '--scan', type=str2bool, dest='scan', required=False, default=True, help='scan address list. ')
    parser.add_argument('-sf', '--scan_file', type=str, dest='scan_file', required=False, default=None, help='scan address list from file. ')
    parser.add_argument('-q', '--quenue', type=str2bool, dest='quenue', required=False, help='consume quenue. ')
    parser.add_argument('-e', '--excel', type=str2bool, dest='excel', required=False, help='make excel file. ')
    parser.add_argument('-m', '--make_file', type=str, dest='make_file', required=False, help='excel file name. ')
    parser.add_argument('-ma', '--make_all', type=str, dest='make_all', required=False, default=False, help='all address to excel. ')
    pargs = parser.parse_args()
    # 扫描地址数据
    if pargs.scan:
        while True:
            t = Arkham()
            # t.run(file='address.list')
            t.run(file='dex_words.list')
        # loop = asyncio.get_event_loop()
        # loop.run_until_complete(t.load_page(file='address.list'))

    if pargs.quenue:
        spend_quenue()

    if pargs.excel:
        make_excel(pargs.make_file, make_all=pargs.make_all) if pargs.make_file else make_excel(make_all=pargs.make_all)

