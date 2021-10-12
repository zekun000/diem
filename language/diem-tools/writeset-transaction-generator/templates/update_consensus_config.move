script {
    use DiemFramework::DiemConsensusConfig;
    fun main(diem_root: signer, _diem_root: signer, config: vector<u8>) {
        DiemConsensusConfig::set(&diem_root, config);
    }
}
