script {
    use DiemFramework::DiemConsensusConfig;
    fun main(diem_root: signer, _diem_root: signer) {
        DiemConsensusConfig::initialize(&diem_root);
    }
}
