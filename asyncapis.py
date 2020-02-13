import asyncio
import typing
from asyncio import AbstractEventLoop

import httpx
import json
import asyncpool
import dataclasses

from httpx.content_streams import RequestData, RequestFiles

from dfsrereinstate.utils import logger
from httpx.auth import AuthTypes
from httpx.config import TimeoutTypes, UnsetType, UNSET
from httpx.models import URLTypes, QueryParamTypes, HeaderTypes, CookieTypes
from linkedin.http import tls
from dfsrereinstate.utils import load_config


@dataclasses.dataclass(eq=False)
class AsyncCall:
    get_results: dict = dataclasses.field(init=False)
    post_results: dict = dataclasses.field(init=False)
    get_inputs: list = dataclasses.field(init=False)
    post_inputs: list = dataclasses.field(init=False)

    def __post_init__(self):
        self.get_results = {}
        self.post_results = {}
        self.get_inputs = []
        self.post_inputs = []
        self.configs = load_config()
        self.log = logger.getLoggerInstance(__name__, self.configs.LOG_LEVEL)

    async def _async_get_api_call(self, i, **kwargs,):
        url = kwargs.get('url')
        retry = kwargs.pop('retry')
        # configs = load_config()
        # log = logger.getLoggerInstance(__name__, configs.LOG_LEVEL)
        async with httpx.AsyncClient(verify=tls.ca_bundle_path()) as client:
            try:
                response = await client.get(**kwargs)
                if response.status_code == 200:
                    try:
                        data = response.json()
                        self.get_results[i] = data
                    except json.decoder.JSONDecodeError:
                        data = response.text
                        self.get_results[i] = data
                        self.log.exception('Error occurred while converting to json object')
                else:
                    self.log.error(f'Status_code: {response.status_code}, response: {response.text}')
            except httpx.exceptions.HTTPError as e:
                if (retry - 1) > 0:
                    kwargs['retry'] = retry - 1
                    await self._async_get_api_call(i, **kwargs)
                else:
                    self.get_results[i] = None
                    self.log.info(f'Error occurred while making api call. Url: {url}. exception message: {e}')

    async def _async_post_api_call(self, i, **kwargs,):
        url = kwargs.get('url')
        retry = kwargs.pop('retry')
        # configs = load_config()
        # log = logger.getLoggerInstance(__name__, configs.LOG_LEVEL)
        async with httpx.AsyncClient(verify=tls.ca_bundle_path()) as client:
            try:
                response = await client.post(**kwargs)
                if response.status_code == 200:
                    try:
                        data = response.json()
                        self.post_results[i] = data
                    except json.decoder.JSONDecodeError:
                        data = response.text
                        self.post_results[i] = data
                        self.log.exception('Error occurred while converting to json object')
                else:
                    self.log.error(f'Status_code: {response.status_code}, response: {response.text}')
            except httpx.exceptions.HTTPError as e:
                if (retry - 1) > 0:
                    kwargs['retry'] = retry - 1
                    await self._async_post_api_call(i, **kwargs)
                else:
                    self.post_results[i] = None
                    print(e)
                    # self.log.exception(f'Error occurred while making api call. Url: {url}. exception message: {e}')

    async def _make_get_api_call(self,
                                 urls_params: list,
                                 loop: AbstractEventLoop,
                                 num_workers: int = 10,
                                 ):
        async with asyncpool.AsyncPool(loop, num_workers=num_workers, name="dfsre-reinstate-pool", logger=self.log,
                                       worker_co=self._async_get_api_call) as pool:
            for i, urls_param in enumerate(urls_params):
                await pool.push(i, **urls_param)
        return self.get_results

    async def _make_post_api_call(self,
                                  urls_params: list,
                                  loop: AbstractEventLoop,
                                  num_workers: int = 10,
                                  ):
        async with asyncpool.AsyncPool(loop, num_workers=num_workers, name="dfsre-reinstate-pool", logger=self.log,
                                       worker_co=self._async_post_api_call) as pool:
            for i, urls_param in enumerate(urls_params):
                await pool.push(i, **urls_param)
        return self.post_results

    def get(self):
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(self._make_get_api_call(urls_params=self.get_inputs, loop=loop))
        finally:
            loop.close()
        self.get_results = {}  # resetting the result value
        self.get_inputs = []  # resetting the result value
        return result

    def post(self):
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(self._make_post_api_call(urls_params=self.post_inputs, loop=loop))
        finally:
            loop.close()
        self.post_results = {}  # resetting the result value
        self.post_inputs = []  # resetting the result value
        return result

    def push_get_http_param(self, url: URLTypes,
                            params: QueryParamTypes = None,
                            headers: HeaderTypes = None,
                            cookies: CookieTypes = None,
                            auth: AuthTypes = None,
                            allow_redirects: bool = True,
                            retry: int = 1,
                            timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET,
                            ):
        self.get_inputs.append({
            'url': url,
            'params': params,
            'headers': headers,
            'cookies': cookies,
            'auth': auth,
            'allow_redirects': allow_redirects,
            'retry': retry,
            'timeout': timeout,
        })

    def push_post_http_param(self, url: URLTypes,
                             data: RequestData = None,
                             files: RequestFiles = None,
                             params: QueryParamTypes = None,
                             headers: HeaderTypes = None,
                             cookies: CookieTypes = None,
                             auth: AuthTypes = None,
                             allow_redirects: bool = True,
                             retry: int = 1,
                             timeout: typing.Union[TimeoutTypes, UnsetType] = UNSET,
                             ):
        self.post_inputs.append({
            'url': url,
            'data': data,
            'files': files,
            'params': params,
            'headers': headers,
            'cookies': cookies,
            'auth': auth,
            'allow_redirects': allow_redirects,
            'retry': retry,
            'timeout': timeout,
        })

