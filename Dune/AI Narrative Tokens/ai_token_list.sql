with all_ai_tokens(name,symbol,token_address,launch_date,description) as (
    values

    ('Worldcoin',
    'WLD',
    0x163f8C2467924be0ae7B5347228CABF260318753,
    date '2023-07-17',
    'Worldcoin is a digital identification platform that aims to provide each person on earth with a convenient way to verify that they are a real human and not a bot or an AI algorithm'
    )

    , ('Arkham',
    'ARKM',
    0x6E2a43be0B1d33b726f0CA3b8de60b3482b8b050,
    date '2023-07-05',
    'Arkham is a blockchain analysis platform that leverages AI to deanonymize blockchain and on-chain data'
    )

    , ('Livepeer',
    'LPT',
    0x58b6A8A3302369DAEc383334672404Ee733aB239,
    date '2018-04-30',
    'Livepeer is the first fully decentralized live video streaming network protocol'
    )

    , ('Verasity',
    'VRA',
    0xF411903cbC70a74d22900a5DE66A2dda66507255,
    date '2021-01-27',
    'designed to fight advertising fraud, provide open access to infrastructure for publishers and advertisers, and reward users for watching video content'
    )

    , ('Sleepless AI',
    'AI',
    0xCf64487276E05afDc3eD669fB5DCEbb17000fD58,
    date '2023-12-27',
    'a Web3+AI gaming platform that aiming to revolutionize the gaming industry by offering unparalleled emotional support and immersive gaming experiences through AI companion games'
    )

    , ('NFPrompt',
    'NFP',
    0x6d453BBBBA06c761Ae091f3BB6ccEB86AE157ab2,
    date '2023-12-20',
    'an AI-driven UGC platform designed for the new generation of Web3 creators, offering AI-powered NFT creation tools, a creator community, and an AI NFT marketplace where users can buy and sell NFTs produced by artificial intelligence.'
    )

    , ('Bittensor',
    'TAO',
    0xD6866576437529850B8322df60C16aAeEb3E3Cf7,
    date '2023-11-26',
    'an open-source protocol that powers a decentralized, blockchain-based machine learning network, rewarding machine learning models for their informational value'
    )

    , ('Injective',
    'INJ',
    0xe28b3B32B6c345A34Ff64674606124Dd5Aceca30,
    date '2020-10-17',
    'layer-1 blockchain building DeFi with AI')

    , ('Render',
    'RNDR',
    0x6De037ef9aD2725EB40118Bb1702EBb27e4Aeb24,
    date '2019-12-12',
    'a distributed graphic processing unit (GPU) rendering network'
    )

    , ('Fetch.ai',
    'FET',
    0xaea46A60368A7bD060eec7DF8CBa43b7EF41Ad85,
    date '2020-10-05',
    'an AI platform constructing a decentralized machine learning network with a crypto economy'
    )

    , ('SingularityNET',
    'AGIX',
    0x5B7533812759B45C2B44C19e320ba2cD2681b542,
    date '2021-04-28',
    'a decentralized marketplace for AI services, enabling collaboration among AI agents'
    )

    , ('Ocean Protocol',
    'OCEAN',
    0x599CF0c7Ab77F932b82aCc30D28E244861fA6c7B,
    date '2022-09-07',
    'Ocean Protocol facilitates data sharing and monetization while maintaining privacy using blockchain technology'
    )
)

select 
    ait.*
    , tki.decimals
from all_ai_tokens ait
inner join tokens.erc20 tki
    on ait.token_address = tki.contract_address
    and ait.symbol = tki.symbol
where tki.blockchain = 'ethereum'
    and ait.symbol in ( -- select some still active tokens
            'FET'
            ,'LPT'
            ,'INJ'
            ,'WLD'
            ,'AGIX'
            ,'RNDR'
            ,'VRA'
        )