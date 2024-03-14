
with L2proxyOnL1(address, name) as (
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
    (0xdfe97868233d1aa22e815a266982f2cf17685a27, 'Optimism: OVM Canonical Sequencer Badrock'),
    (0xFF00000000000000000000000000000000000010, 'Optimism: OVM Canonical Proposer Badrock'),

    (0xabea9132b05a70803a4e85094fd0e1800777fbef, 'ZkSync Lite: ZkSync Contract'),
    (0x18c208921F7a741510a7fc0CfA51E941735DAE54, 'ZkSync Lite: ZkSync New Operetor'),
    (0xda7357bBCe5e8C616Bc7B0C3C86f0C71c5b4EaBb, 'ZkSync Lite: ZkSync Old Operator'),

    (0x4e4943346848c4867F81dFb37c4cA9C5715A7828, 'ZkSync ERA: MultiSig'),
    (0x112200EaA6d57120c86B8b51a8b6049d56B82211, 'ZkSync ERA: Active Validator OL'),
    (0x3527439923a63F8C13CF72b8Fe80a77f6e572092, 'ZkSync ERA: Active Validator NE'),
    (0x32400084C286CF3E17e7B677ea9583e60a000324, 'ZkSync ERA: Diamond Proxy'),
    (0x3dB52cE065f728011Ac6732222270b3F2360d919, 'ZkSync ERA: Validator Timelock OL'),
    (0x57891966931Eb4Bb6FB81430E6cE0A03AAbDe063, 'ZkSync ERA: ERC 20 Bridge'),
    (0xa0425d71cB1D6fb80E65a5361a04096E0672De03, 'ZkSync ERA: Validator Timelock NE'),

    (0x148Ee7dAF16574cD020aFa34CC658f8F3fbd2800, 'Polygon zkEVM: Sequencer'), 
    (0x5132A183E9F3CB7C848b0AAC5Ae0c4f0491B7aB2, 'Polygon zkEVM: Main Contract'),
    (0xdA87c4a76922598Ac0272F4D9503a35071D686eA, 'Polygon zkEVM: Aggregator'), 
    (0x2a3DD3EB832aF982ec71669E178424b10Dca2EDe, 'Polygon zkEVM: Bridge'), 

    (0x2C169DFe5fBbA12957Bdd0Ba47d9CEDbFE260CA7, 'StarkNet: Operator'), 
    (0xc662c410C0ECf747543f5bA90660f6ABeBD9C8c4, 'StarkNet: Core Contract'), 
    (0x96375087b2F6eFc59e5e0dd5111B4d090EBFDD8B, 'StarkNet: Memory Page Fact Registry'), 
    (0xae0Ee0A63A2cE6BaeEFFE56e7714FB4EFE48D419, 'StarkNet: Starknet ETH Bridge'), 
    (0x9F96fE0633eE838D0298E8b8980E6716bE81388d, 'StarkNet: Starkgate L1 DAI Bridge'), 
    (0x283751A21eafBFcD52297820D27C1f1963D9b5b4, 'StarkNet: Starkgate WBTC Bridge'), 
    (0xF6080D9fbEEbcd44D89aFfBFd42F098cbFf92816, 'StarkNet: Starkgate USDC Bridge'), 
    (0xbb3400F107804DFB482565FF1Ec8D8aE66747605, 'StarkNet: Starkgate USDT Bridge'), 
    (0xFD14567eaf9ba941cB8c8a94eEC14831ca7fD1b4, 'StarkNet: Sharp Blockchain writer'), 

    (0xFf00000000000000000000000000000000008453, 'Base: Nullifier'), 
    (0x642229f238fb9dE03374Be34B0eD8D9De80752c5, 'Base: Proposer'), 
    (0x56315b90c40730925ec5485cf004d835058518A0, 'Base: L2OutputOracle'), 
    (0x49048044D57e1C92A77f79988d21Fa8fAF74E97e, 'Base: OptimismPortal'), 
    (0x608d94945A64503E642E6370Ec598e519a2C1E53, 'Base: L1ERC721Bridge'), 
    (0x866E82a600A1414e583f7F13623F1aC5d58b0Afa, 'Base: L1CrossDomainMessenger'), 

    (0xcF2898225ED05Be911D3709d9417e86E0b4Cfc8f, 'Scroll: Sequencer 1'),
    (0x2ce8B4A516ebBc8B425764a867B742F76C2244c7, 'Scroll: Sequencer 2'),
    (0x356483dC32B004f32Ea0Ce58F7F88879886e9074, 'Scroll: Proposer 1'),
    (0x69d79Fc4Ae89E4DA80D719e26a435621F75B7f06, 'Scroll: Proposer 2'),
    (0xa13BAF47339d63B743e7Da8741db5456DAc1E556, 'Scroll: Chain'),
    (0x6774Bcbd5ceCeF1336b5300fb5186a12DDD8b367, 'Scroll: Messanger'),
    (0xF8B1378579659D8F7EE5f3C929c2f3E332E41Fd6, 'Scroll: L1gatewayRouter'),

    (0x415c8893D514F9BC5211d36eEDA4183226b84AA7, 'Blast: Sequencer'),
    (0x082b616Ec99167B2FEdee053F07db6795D4dA821, 'Blast: Proposer'),
    (0x826D1B0D4111Ad9146Eb8941D7Ca2B6a44215c76, 'Blast: L2 Output Contract'),
    (0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115, 'Blast: L1 standard bridge'),
    (0x697402166Fbf2F22E970df8a6486Ef171dbfc524, 'Blast: L1 Blast Bridge'),
    (0x98078db053902644191f93988341E31289E1C8FE, 'Blast: Yeld Manager'),
    (0xa230285d5683C74935aD14c446e137c8c8828438, 'Blast: USD Yeld Manager')
)

select 
    address 
    , name
    , split(name,':')[1] as l2chain
from L2proxyOnL1