
with L2proxyOnL1(address, name, l2chain) as (
    values
    (0x4c6f947Ae67F572afa4ae0730947DE7C874F95Ef, 'Arbitrum: Sequencer Inbox'), 
    (0x51de512aa5dfb02143a91c6f772261623ae64564, 'Arbitrum: Validator1'),
    (0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6, 'Arbitrum: Sequencer Inbox NITRO'),
    (0x72Ce9c846789fdB6fC1f34aC4AD25Dd9ef7031ef, 'Arbitrum: Gateway Router'),

    (0x211E1c4c7f1bF5351Ac850Ed10FD68CFfCF6c21b, 'Arbitrum Nova: Sequencer Inbox'), 
    (0xc4448b71118c9071Bcb9734A0EAc55D18A153949, 'Arbitrum Nova: Delayed Inbox'),
    (0xD4B80C3D7240325D18E645B49e6535A3Bf95cc58, 'Arbitrum Nova: Outbox'),

    (0x4BF681894abEc828B212C906082B444Ceb2f6cf6, 'Optimism: OVM Canonical Transaction Chain OLD'),
    (0x5E4e65926BA27467555EB562121fac00D24E9dD2, 'Optimism: OVM Canonical Transaction Chain NEW'),
    (0xBe5dAb4A2e9cd0F27300dB4aB94BeE3A233AEB19, 'Optimism: OVM State Commitment Chain New'),

    (0xabea9132b05a70803a4e85094fd0e1800777fbef, 'ZkSync: ZkSync Contract'),
    (0x18c208921F7a741510a7fc0CfA51E941735DAE54, 'ZkSync: ZkSync New Operetor'),
    (0xda7357bBCe5e8C616Bc7B0C3C86f0C71c5b4EaBb, 'ZkSync: ZkSync Old Operator'),
)