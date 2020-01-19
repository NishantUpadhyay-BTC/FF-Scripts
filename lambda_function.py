import asyncio
from pyppeteer import launch
import sentry_sdk
import pprint
import psycopg2
import http.client
from datetime import date
import os
import boto3
import json
import smtplib
import email.message
from datetime import datetime
from pbx_gs_python_utils.utils.Process      import Process
from load_dependency import load_dependency

sentry_sdk.init("https://46eaec2e30c748c9aedcc71611b57e5d@sentry.io/1866692")
number_error = "not valid number"
exception_tbl = "freight_forwarder_scraping_exceptionlog"
transaction_tbl = "freight_forwarder_scraping_transactionslog"
scrapeddata_tbl = "freight_forwarder_scraping_scrapeddata"

load_dependency('pyppeteer')

path_headless_shell          = '/tmp/pyppeteer/headless_shell'
path_headless_shell_so1 = '/tmp/pyppeteer/swiftshader/libEGL.so'
path_headless_shell_so2 = '/tmp/pyppeteer/swiftshader/libGLESv2.so'

os.environ['PYPPETEER_HOME'] = '/tmp'
Process.run("chmod", ['+x', path_headless_shell])
Process.run("chmod", ['+x', path_headless_shell_so1])
Process.run("chmod", ['+x', path_headless_shell_so2])

# async def send_email(data):
#     server = smtplib.SMTP('smtp.gmail.com:587')
#
#     email_content = f"""
#     <html>
#     <head>
#     </head>
#     <body>
#     <p>Hi Admin,</p>
#     <p>This is to inform you that, system has caught an exception on FreightForwarder. Here is the details:</p>
#     <strong>1. Log type: </strong> {data['logtype']}<br>
#     <strong>2. Raised Exception: </strong> {data['raised_exception']}<br>
#     <strong>3. URL: </strong> {data['url']}<br>
#     <strong>4. HBL: </strong> {data['hbl_container']}<br>
#     </body>
#     </html>
#
#     """
#
#     msg = email.message.Message()
#     msg['Subject'] = 'FreightForwarder - Exception Raised'
#
#     msg['From'] = 'botreetesting@gmail.com'
#     recipients = os.environ['RECIPIENTS'].split(',')
#     # 'nishantupadhyay@botreetechnologies.com'
#     msg['To'] = os.environ['RECIPIENTS']
#     password = "botree123"
#     msg.add_header('Content-Type', 'text/html')
#     msg.set_payload(email_content)
#
#     s = smtplib.SMTP('smtp.gmail.com: 587')
#     s.starttls()
#
#     # Login Credentials for sending the mail
#     s.login(msg['From'], password)
#
#     s.sendmail(msg['From'], recipients, msg.as_string())

def connect_db(host="", database="", user="", password=""):
    con = psycopg2.connect(
            host = os.environ["DB_HOST"],
            database = os.environ["DB_NAME"],
            user = os.environ["DB_USER"],
            password = os.environ["DB_PASSWORD"])
    # con = psycopg2.connect(
    #     host = 'localhost',
    #     database = 'freightforwarder',
    #     user = 'postgres',
    #     password = 'root')
    return con


def insert(con, table, **kwargs):
    cur = con.cursor()

    placeholders = ', '.join(['%s'] * len(kwargs))
    columns = ', '.join(kwargs.keys())
    query = "Insert Into %s (%s) Values (%s)" % (table, columns, placeholders)
    cur.execute(query, list(kwargs.values()))

    cur.close()
    con.commit()


async def update(con, table, **kwargs):
    cur = con.cursor()

    columns = ', '.join(f"{column}='{value}'" for column,
                        value in kwargs['data'].items())
    query = "update %s set %s where %s='%s'" % (
        table, columns, kwargs['condition_column'], kwargs['code'])
    cur.execute(
        query, (table, columns, kwargs['condition_column'], kwargs['code']))

    cur.close()
    con.commit()


async def transaction_log(con, table, logtype="", request="", response="", url="", hbl_container=""):
    data = {"logtype": logtype, "request": request,
            "response": response, "url": url, "hbl_container": hbl_container}
    status = True
    try:
        insert(con, table, **data)
    except Exception as e:
        print(e)
        status = False

    return status


async def exception_log(con, table, logtype="", raised_exception="", url="", hbl_container=""):
    data = {"logtype": logtype, "raised_exception": raised_exception,
            "url": url, "hbl_container": hbl_container}
    status = True
    try:
        insert(con, table, **data)
        # await send_email(data)
    except Exception as e:
        print(e)
        status = False

    return status


async def get_browser():
    # return await launch(ignoreHTTPSErrors=True)
    return await launch(executablePath=path_headless_shell,
                        ignoreHTTPSErrors=True,
                        headless=True,
                         autoClose =False,
                         args=['--no-sandbox',
                               '--single-process','--ignore-certificate-errors', '--no-zygote',])


async def get_page(browser, url):
    page = await browser.newPage()
    await page.goto(url, timeout=1000000)
    return page


async def get_html(code):
    url = "http://www.ftcargoline.com/search_main.php"
    """
        This is for direct post request to server because website is not working in headless browser.
        This function send request to server and get html page as response.
        Return html page.
    """
    url_part = url.split("/")
    host, file_name = url_part[2], url_part[3]
    conn = http.client.HTTPConnection(host)

    payload = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"bl_no\"\r\n\r\n"+code + \
        "\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"submit\"\r\n\r\nSearch\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"

    headers = {
        'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
        'cache-control': "no-cache",
        'postman-token': "18aae73b-dc65-26be-4416-6c53a9151612"
    }

    conn.request("POST", "/{}".format(file_name), payload, headers)
    return conn.getresponse().read().decode('utf-8')


async def extract_data_1(browser, page, code, **kwargs):
    await page.type("#username", "A00059")
    await page.type("#passwd", "ASF021")
    await page.click('input[type=submit]')
    await page.waitForSelector('#inbound_tab')
    await page.click('#inbound_tab')
    await (await page.J("#so_bl_no")).type(code)
    await page.click("input[type=submit]")
    await asyncio.sleep(5)
    await page.waitForSelector('#yui-dt0-paginator1')
    raise_exception = await page.JJ('table tr td:nth-child(6)')

    excepts = []
    for i in raise_exception:
        excepts.append(await page.evaluate('(element) => element.textContent', i))
    if excepts[-1] == 'null':
        raise Exception(number_error)
    date = await page.Jeval('table tr td:nth-child(10)', '(element) => element.textContent')
    res = datetime.strptime(date+'/2020', '%d/%b/%Y')
    return {"delievered_on": res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S') }


# async def extract_data_2(browser, page, code):
#
#     evaluate_js = '(element) => element.textContent'
#
#     await asyncio.wait([
#         page.click("#INDEX_cargoS02"),
#         page.waitForSelector('#INDEX_cargMtNo')]
#     )
#     await page.type("#INDEX_mblNo", code)
#     await asyncio.wait([
#         page.click("#INDEX_btnRetrieveCargPrgsInfo"),
#         page.waitForSelector('#MYC0405101Q_mblNoTab1')]
#     )
#     await page.Jeval('#MYC0405101Q_mblNoTab1', '() => document.getElementById("MYC0405101Q_mblNoTab1").value = ""')
#     await page.type("#MYC0405101Q_hblNoTab1", code)
#     await page.select("#MYC0405101Q_blYyTab1", "2021")
#
#     for i in range(5):  # check last 5 year
#         await page.select("#MYC0405101Q_blYyTab1", str(date.today().year - i))
#         await click_button(page)  # search button funtion
#         no_result = await page.Jeval('#MYC0405101Q_resultTab1 tbody', evaluate_js)
#         if not no_result:
#             break
#     try:
#         await page.waitForSelector('#MYC0405102Q_resultListL tr:last-child')
#     except Exception:
#         raise Exception("ssdsdsdsdsd")
#     await asyncio.sleep(5)
#     table_pages = await page.JJ(".paging:last-child li")
#     # '-1' is for first page is repeat in list
#     for table_page in table_pages[:-1]:
#         table = await page.J('#MYC0405102Q_resultListL')
#         col_2_data = await table.JJ('tr td:nth-child(2)')
#         dates = await table.JJ('tr td:nth-child(5)')
#         result = [await page.evaluate(evaluate_js, date) for col_2, date in zip(col_2_data, dates) if "반출신고" in await page.evaluate(evaluate_js, col_2)]
#         if result:
#             break
#         else:
#             await asyncio.gather(*[
#                 page.click(table_page),
#                 page.waitForSelector('#MYC0405102Q_resultListL tr:last-child')], return_exceptions=False
#             )
#             await asyncio.sleep(5)
#     return {"delievered_on": str(result[0])}
#

async def extract_data_3(browser, page, code, **kwargs):
    await page.type("#ShipmentHousebillNumberTextbox", code)
    await asyncio.gather(*[
        page.click('#FindBtn'),
        page.waitForNavigation()], return_exceptions=False)
    table = await page.J('#Milestones_MilestonesPanel_MilestonesGrid')
    evaluate_js = '(element) => element.textContent'
    try:
        col_2_data = await table.JJ('tr td:nth-child(2)')
    except Exception:
        raise Exception(number_error)
    dates = await table.JJ('tr td:nth-child(3)')
    all_status = await table.JJ('tr td:nth-child(4)')
    description = "Delivery order has been released"
    STATUS = ['Completed', 'Completed Late']
    result = [await page.evaluate(evaluate_js, date) for col_2, date, status in zip(col_2_data, dates, all_status) if description in await page.evaluate(evaluate_js, col_2) and await page.evaluate(evaluate_js, status) in STATUS]
    res = datetime.strptime(",".join([date.replace('\t', '').replace('\n', '') for date in result]), '%d-%b-%y %I:%M')
    result = {"delievered_on":res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S') }
    return result


async def extract_data_4(browser, page, code):
    await asyncio.gather(*[
        page.waitForSelector('#mat-input-0')], return_exceptions=False)
    await (await page.J('#mat-input-0')).type(code)
    try:
        await asyncio.gather(*[
            page.click('button[type=submit]'),
            page.waitForSelector('div[class="description"]')
        ], return_exceptions=False)
    except Exception:
        raise Exception(number_error)
    pickup = await page.JJ('div[class="description"]')
    pickup_date = await page.Jeval('div[class="ng-star-inserted"]', '(element) => element.textContent')

    # Extract data
    result = {'GOODS_PICKED_UP': ''}
    allpickups = []
    for centers in pickup:
        allpickups.append(await page.evaluate('(element) => element.textContent', centers))
    result["GOODS_PICKED_UP"] = allpickups[-1]
    result["GOODS_PICKED_UP"] = result["GOODS_PICKED_UP"] + \
        ","+pickup_date.split(" ")[-2]
    res = datetime.strptime(result['GOODS_PICKED_UP'].split(',')[-1], '%d/%m/%Y')
    result = {"GOODS_PICKED_UP":res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S') }
    return result


async def extract_data_5(browser, page, code):
    await page.type('#cbjsearch', code)
    try:
        await asyncio.gather(*[
            page.keyboard.press("Enter"),
            page.waitForSelector('.details')
        ], return_exceptions=False)
    except Exception:
        raise Exception(number_error)
    await page.click('.details')
    await asyncio.sleep(5)
    pop_up_page = (await browser.pages())[-1]
    date = await pop_up_page.Jeval('.resulttables tr:nth-child(8) td:nth-last-child(1)', '(element) => element.textContent')
    res = datetime.strptime(date, '%d/%m/%Y')
    return {"delievered_on": res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S') }


async def extract_data_6(browser, page, code):
    await (await page.J('#Container')).type(code)
    await asyncio.gather(*[
        page.click('.btn-default'),
        page.waitForNavigation()], return_exceptions=False)
    dispatch_date = await page.JJ('table[class="table table-striped"] tr td:nth-child(12)')
    hbl = await page.JJ('table[class="table table-striped"] tr td:nth-child(13)')
    if not dispatch_date:
        raise Exception(number_error)
    # Extract data
    result = {'delievered_on': [], 'ams_hbl_number': []}
    for date, number in zip(dispatch_date, hbl):
        result['delievered_on'].append(await page.evaluate('(element) => element.textContent', date))
        result['ams_hbl_number'].append(await page.evaluate('(element) => element.textContent', number))
    delievered_on = []
    for i in result['delievered_on']:
        res = datetime.strptime('06.01.20', '%d.%m.%y')
        delievered_on.append(res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S'))
    result['delievered_on'] = delievered_on
    return result


async def extract_data_7(browser, page, code, **kwargs):
    await page.goto(f'http://www.penanshin.com/tracking.php?search_by=hbl&track_num={code}')
    not_valid = await page.Jeval("h4+ .col-sm-6 .detail", "(tag) => tag.textContent")
    if "INVALID" in not_valid:
        raise Exception(number_error)
    output = await page.Jeval(".col-sm-12+ .col-sm-6 .detail", "(tag) => tag.textContent")
    res = datetime.strptime(output.strip().split(' ')[0], '%d/%m/%Y')
    return {"delievered_on": res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S') }


async def extract_data_8(browser, page, code, **kwargs):
    # select value from dropdown and enter FF number
    await page.select("#P102_TYPE", "HBL")
    await page.type('#P102_NO', code)

    # Search and get data
    try:
        await asyncio.gather(*[
            page.click('.t-Button--hot'),
            page.waitForSelector('#P102_JSHP_ESTIMATED_DELIVERY_DATE')
        ], return_exceptions=False)
    except Exception:
        raise Exception(number_error)

    date = await page.Jeval('#P102_JSHP_ESTIMATED_DELIVERY_DATE', '(element) => element.textContent')
    res = datetime.strptime(date, '%d-%b-%y')
    return {"delievered_on": res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S') }


async def extract_data_9(browser, page, code, **kwargs):
    url = "http://www.cargobd.com/asycuda/asyc/Forms/AsycudaRequestAgent.php?uid=13&pid=11#"
    await page.goto(url)
    await asyncio.sleep(5)

    # Search and get data
    try:
        await (await page.J('#SearchTextWise')).type(code)
        await asyncio.gather(*[
            page.click('#SearchSearchTextWise'),
            page.waitForSelector(
                '.datagrid-btable td[field="DELIVERY_DATE"]'),
        ], return_exceptions=False)
    except Exception:
        raise Exception(number_error)
    result = await page.Jeval('.datagrid-btable td[field="DELIVERY_DATE"]', '(element) => element.textContent')
    res = datetime.strptime(result, '%Y-%m-%d')
    return {"delievered_on": res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S') }

async def extract_data_10(browser, page, code):

    await page.select(".apex-item-select", "HBL")
    await (await page.J('.apex-item-text')).type(code)
    try:
        await asyncio.gather(*[
            page.click('.t-Button--hot'),
            page.waitForSelector('dd[class="t-AVPList-value"]')
        ], return_exceptions=False)
    except Exception as e:
        raise Exception(number_error)

    fetched_data = await page.JJ('dd[class="t-AVPList-value"]')

    # Extract data
    result = {}
    alldata = []
    finaldata = []
    counter = 0
    for data in fetched_data:
        counter += 1
        if counter in [1, 3, 17, 18]:
            alldata.append(await page.evaluate('(element) => element.textContent', data))
            alldata = alldata[0].split('\n  ')
            alldata = alldata[1].split('\n')
            finaldata.append(alldata[0])
        alldata = []
    result["do_given_date"] = finaldata[2]
    result["delievered_on"] = finaldata[3]
    do_date = datetime.strptime(result["do_given_date"], '%d-%b-%y')
    d_date = datetime.strptime(result["delievered_on"], '%d-%b-%y')
    result = {"do_given_date": do_date.strftime('%Y') + '-' + do_date.strftime('%m') + '-' + do_date.strftime('%d') + ' ' + do_date.strftime('%H') + ':' + do_date.strftime('%M') + ':' + do_date.strftime('%S'),"delievered_on":d_date.strftime('%Y') + '-' + d_date.strftime('%m') + '-' + d_date.strftime('%d') + ' ' + d_date.strftime('%H') + ':' + d_date.strftime('%M') + ':' + d_date.strftime('%S')}
    return result


async def extract_data_11(browser, page, code):
    await page.setContent(await get_html(code))
    wrong = await page.JJ('.wrong')
    if wrong:
        raise Exception("not valid number")
    result = await page.Jeval('tr:nth-child(6) td+ td', '(element) => element.textContent')
    res = datetime.strptime(result.split(' ')[0], '%b-%d-%Y')
    return {"delievered_on": res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S') }


async def extract_data_12(browser, page, code):
    await (await page.J('#ContainerOrLoadNumber')).type(code)
    await asyncio.gather(*[
        page.click('.btn-default'),
        page.waitForNavigation()
    ], return_exceptions=False)

    c = 0
    am = await page.JJ('p')
    for i in am:
        if await page.evaluate('(element) => element.textContent', i) == 'Page 1 of 1':
            c = 1
    if c != 1:
        raise Exception("not valid number")
    else:
        piece = await page.JJ('table[class="table"] tr td:nth-child(5)')
        weigh = await page.JJ('table[class="table"] tr td:nth-child(6)')

        result = {'pieces': [],
                  'weight': [], 'pu_date': [], 'go_date': [], "pu_number": [], 'ams_hbl_number': []}
        for piece_data, weight_data in zip(piece, weigh):
            result["pieces"].append(await page.evaluate('(element) => element.textContent', piece_data))
            result["weight"].append(await page.evaluate('(element) => element.textContent', weight_data))
        for pu_number in await page.JJ('table[class="table"] tr td:nth-child(1)'):
            refactored_pu_number = await page.evaluate('(element) => element.textContent', pu_number)
            result['pu_number'].append(refactored_pu_number.replace(" ", ""))
        for pu_number in result['pu_number']:
            await asyncio.wait([
                page.goto(
                    'http://lookup.terminaltransfer.com/Details?pickupNumber=%s' % pu_number),
                page.waitForNavigation()]
            )
            pu_date_each = await page.JJ('.col-lg-3:nth-child(4)')
            go_date_each = await page.JJ('.col-lg-3:nth-child(3)')
            for pu_date, go_date in zip(pu_date_each, go_date_each):
                pu_date = await page.evaluate('(element) => element.textContent', pu_date)
                pu_date = pu_date.split("\n")
                pu_date = pu_date[-1].split(' ')[0]
                result["pu_date"].append(pu_date)
                go_date = await page.evaluate('(element) => element.textContent', go_date)
                go_date = go_date.split("\n")
                go_date = go_date[-1].split(' ')[0]
                result["go_date"].append(go_date)
                hbl = await page.Jeval(".property:nth-child(11)", '(element) => element.textContent')
                result['ams_hbl_number'].append(hbl.split(":")[-1])
                await asyncio.wait([
                    page.goBack(),
                    page.waitForNavigation()]
                )
        pu_date = []
        for i in result['pu_date'][:-1]:
            res = datetime.strptime(i, '%m/%d/%Y')
            pu_date.append(res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S'))
        result['pu_date'] = pu_date
        return result


async def extract_data_14(browser, page, code, **kwargs):
    await asyncio.wait([
        page.evaluate('()=> window.location.href = "./"'),
        page.waitForNavigation(waitUntil='networkidle0')]
    )
    await page.type('#searchBLNo', code)
    try:
        await asyncio.gather(*[
            page.click('#btn_search'),
            page.waitForSelector('.ui-row-ltr')
        ], return_exceptions=False)
    except Exception as e:
        raise Exception(number_error)
    await asyncio.gather(*[
        page.click('.ui-row-ltr', clickCount=2),
        page.waitForSelector('#SI_CargoReleaseDate'),
    ], return_exceptions=False)
    await asyncio.sleep(3)
    d = await page.Jeval('#SI_DOReleaseDate', '(element) => element.value')
    a = await page.Jeval('#SI_CargoReleaseDate', '(element) => element.value')
    return {'delievered_on': d, 'do_given_date': a}


async def extract_data_15(browser, page, code):
    await asyncio.wait([
        page.waitForSelector('#searchByDropdown')]
    )
    await page.select("#searchByDropdown", "container")
    await (await page.J('#searchValueField')).type(code)
    try:
        await asyncio.gather(*[
            page.click('input[type="submit"]'),
            page.waitForSelector(
                'table[class="spreadsheet"] tr td:nth-child(2)')
        ], return_exceptions=False)
    except Exception as e:
        raise Exception(number_error)
    ams = await page.JJ('table[class="spreadsheet"] tr td:nth-child(2)')
    shipdate = await page.JJ('table[class="spreadsheet"] tr td:nth-child(12)')

    # Extract data
    result = {'ams_hbl_number': [], 'delievered_on': [], 'go_date': [await page.Jeval('div[class="splitColumn fr"] table[class="dataDisplay"] tr:nth-child(9) td:nth-child(2)', '(element) => element.textContent')]}
    for amsdata, shipdata in zip(ams, shipdate):
        result["ams_hbl_number"].append(await page.evaluate('(element) => element.textContent', amsdata))
        result["delievered_on"].append(await page.evaluate('(element) => element.textContent', shipdata))
    result['go_date'] = result['go_date']*len(result['ams_hbl_number'])
    delievered_on = []
    for i in result['delievered_on']:
        res = datetime.strptime(i, '%d/%b/%Y')
        delievered_on.append(res.strftime('%Y') + '-' + res.strftime('%m') + '-' + res.strftime('%d') + ' ' + res.strftime('%H') + ':' + res.strftime('%M') + ':' + res.strftime('%S'))
    result['delievered_on'] = delievered_on
    return result


async def extract(browser, name, url, code, function_dict):
    page = await get_page(browser, url)
    data = await function_dict[name](browser, page, code)
    return data


async def extract_site(browser, name, url, code, function_dict, con, condition_col):
    try:
        data = await extract(browser, name, url, code, function_dict)
        print(data)
        await transaction_log(con, transaction_tbl, logtype="scrapping", request=code, response=str(data), url=str(url), hbl_container=code)
        status = "ready to push" if not any(
            row in data.values() for row in ('', [])) else "in process"

        if name in ['6', '12', '15'] and status == "ready to push":
            if False in [any(col for col in v if col) for k, v in data.items()]:
                status = "in process"
            first_row = {column: str(value[0])
                         for column, value in data.items()}
            first_row['status'] = status
            first_row['url'] = str(url)
            await update(con, scrapeddata_tbl, data=first_row, condition_column=condition_col, code=code)
            m = []
            for i in range(len(list(data.values())[0])-1):
                q = []
                for val in data.values():
                    q.append(str(val[i+1]))

                # status = "in process" if "" in q else "ready to push"
                q.extend([code, status, str(url)])
                m.append(str(tuple(q)))
            values = ",".join(m)
            cursor = con.cursor()
            cursor.execute(
                f"select count(*) from {scrapeddata_tbl} where container_number='{code}';")
            row_count = cursor.fetchone()[0]
            print(row_count)
            if row_count > 1:
                for i in range(len(list(data.values())[0])-1):
                    update_data = {column: str(value[i+1]) for column, value in data.items()}
                    # status = "in process" if "" in update_data.values() else "ready to push"
                    update_data['status'] = status
                    # await update(con, scrapeddata_tbl, data=update_data, condition_column="ams_hbl_number", code=update_data['ams_hbl_number'])
                    await update(con, scrapeddata_tbl, data=first_row, condition_column="ams_hbl_number", code=first_row['ams_hbl_number'])
                    print(update_data)
            else:
                data['container_number'] = code
                data['status'] = status
                data['url'] = str(url)
                cursor.execute(
                    f"insert into {scrapeddata_tbl} ({','.join([col for col in data.keys()])}) values {values};")
            cursor.close()
            con.commit()
        else:
            data['status'] = status
            data['url'] = str(url)
            await update(con, scrapeddata_tbl, data=data,
                          condition_column=condition_col, code=code)
        print(status)
        return True
    except Exception as e:
        print("Error----------------------------", e)
        if not number_error == str(e):
            await update(con, scrapeddata_tbl, data={'status':"pending"},condition_column=condition_col, code=code)
            await exception_log(con, exception_tbl, logtype="scrapping", raised_exception=str(e), url=str(url), hbl_container=code)
        return False


async def extract_all_url(con, givendata, function_dict, results):
    print("before data:",results)
    browser = await get_browser()
    hbl_sites = ['1', '2', '3', '4', '5', '7', '8', '9', '10', '11', '15']
    container_sites = ['6', '12', '14']
    if results['url'] not in ['None',None,'none',""]:
        url = results['url']
        givendata = {k:v for k,v in givendata.items() if url in v}
        if not givendata:
            raise Exception("not valid url")
    for i, (name, url) in enumerate(givendata.items()):
        print(name,url)
        col_code = ([results['ams_hbl_number'],"ams_hbl_number"] if results['ams_hbl_number'] not in ['None',None,'none',""] else [results['ophbl_number'],"ophbl_number"]) if name in hbl_sites else [results['container_number'],"container_number"]
        code, condition_col = col_code[0],col_code[1]
        cursor = con.cursor()
        cursor.execute(f"select status from {scrapeddata_tbl} where {condition_col}='{code}'")
        l_tmp = [row[0] for row in cursor.fetchall()]
        code_status = True if (("ready to push" in l_tmp) or ("completed" in l_tmp)) else False
        print(code_status)
        cursor.close()
        if code_status:
            status = True
            break
        if code not in ['None',None,'none',""]:
            try:
                await update(con, scrapeddata_tbl, data={'status': "in process"},
                    condition_column=condition_col, code=code)
                status = await extract_site(browser, name, url, code, function_dict, con, condition_col)
                if status:
                    break
            except Exception as e:
                print("An Error Occured : ", e.__class__.__name__, f"({e})")

    if not status:
        try:
            await update(con, scrapeddata_tbl, data={'status': "not found"},
                         condition_column=condition_col, code=code)
        except Exception as e:
            print(e)
            await exception_log(con, exception_tbl, logtype="scrapping", raised_exception="number not found in any website", url="", hbl_container=code)
    await browser.close()
    return None


def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    queue_url = os.environ["QUEUE_URL"]
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    result = event['Records'][0]['body']
    json_res = dict(json.loads(result))
    print("Below is the number")
    print(json_res)

    function_dict = {
                    "1": extract_data_1,
                     #"2": extract_data_2,
                     "3": extract_data_3,
                     "4": extract_data_4,
                     "5": extract_data_5,
                     "6": extract_data_6,
                     "7": extract_data_7,
                     "8": extract_data_8,
                     "9": extract_data_9,
                     "10": extract_data_10,
                     "11": extract_data_11,
                     "12": extract_data_12,
                     "14": extract_data_14,
                     "15": extract_data_15,
                     }
    givendata = {
        "1": "http://tracking.topconcept.com.hk:81/login.php",
        # "2": "https://unipass.customs.go.kr/csp/index.do",
        "3": "http://tracking.consolalliance.com.au/Login/Login.aspx?ReturnUrl=Default.aspx",
        "4": "https://nvogo.nvoconsolidation.com/tracker",
        "5": "https://multitrack.multifreight.com/scripts/cgiip.exe/WService=daygard/mfweb?action=search&searchform=quick1&page=qsearch&view=main",
        "6": "https://hls-info.mytcigroup.com/sendungsabfrage.php",
        "7": "http://www.penanshin.com/",
        "8": "http://182.54.217.38:17001/apex/f?p=103:102:0::::P0_GROUP_RID:51",
        "9": "http://www.cargobd.com/asycuda/login.php?link=0&msg=safiya",
        "10": "http://efmapp4.fmgloballogistics.com:17001/ords/f?p=321:209:::NO:::",
        "11": "http://www.ftcargoline.com/search_main.php",
        "12": "http://lookup.terminaltransfer.com",
        "14": "http://webtracker.pcdlogistics.com/etrack_quanterm/ShipmentTrackingQuanterm.html",
        "15": "https://cwportal.stgusa.com/warehousingSTG/warehousing?event=TRACKING_RUN",
    }

    try:

        con = connect_db(host="localhost", database="freightforwarder")
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(
    	   extract_all_url(con, givendata, function_dict, json_res))
        con.close()
        return {
            "final_data": result
        }
    except Exception as e:
        print(e)
        con.close()
