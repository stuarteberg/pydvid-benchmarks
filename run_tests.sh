set -x
echo ""
echo "***********************************************************************"
echo ""
python usecase_benchmarks.py master http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale "[(0,1000,1000,1000), (1,3000,3000,2000)]" "(1,1000,1000,1000)" "(1,1000,1000,1000)" "1" --cwd=/groups/flyem/proj/cluster/dvid_testing
echo ""
echo "***********************************************************************"
echo ""
python usecase_benchmarks.py master http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale "[(0,1000,1000,1000), (1,3000,3000,2000)]" "(1,1000,1000,1000)" "(1, 500, 500, 500)" "1" --cwd=/groups/flyem/proj/cluster/dvid_testing
echo ""
echo "***********************************************************************"
echo ""
python usecase_benchmarks.py master http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale "[(0,1000,1000,1000), (1,3000,3000,2000)]" "(1, 500, 500, 500)" "(1, 250, 250, 250)" "1" --cwd=/groups/flyem/proj/cluster/dvid_testing
echo ""
echo "***********************************************************************"
echo ""
python usecase_benchmarks.py master http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale "[(0,1000,1000,1000), (1,3000,3000,2000)]" "(1,1000,1000,1000)" "(1, 500, 500, 500)" "8" --cwd=/groups/flyem/proj/cluster/dvid_testing
echo ""
echo "***********************************************************************"
echo ""
python usecase_benchmarks.py master http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale "[(0,1000,1000,1000), (1,3000,3000,2000)]" "(1, 500, 500, 500)" "(1, 250, 250, 250)" "8" --cwd=/groups/flyem/proj/cluster/dvid_testing
echo ""
echo "***********************************************************************"
echo ""
python usecase_benchmarks.py master http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale "[(0,1000,1000,1000), (1,3000,3000,2000)]" "(1, 500, 500, 500)" "(1, 500, 500, 500)" "1" --cwd=/groups/flyem/proj/cluster/dvid_testing
echo ""
echo "***********************************************************************"
echo ""
python usecase_benchmarks.py master http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale "[(0,1000,1000,1000), (1,3000,3000,2000)]" "(1, 500, 500, 500)" "(1, 250, 250, 250)" "1" --cwd=/groups/flyem/proj/cluster/dvid_testing
echo ""
echo "***********************************************************************"
echo ""
python usecase_benchmarks.py master http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale "[(0,1000,1000,1000), (1,3000,3000,2000)]" "(1, 500, 500, 500)" "(1, 250, 250, 250)" "8" --cwd=/groups/flyem/proj/cluster/dvid_testing
echo ""
echo "***********************************************************************"
echo ""
