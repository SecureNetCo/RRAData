/**
 * 스마트 프리페치 시스템
 * 2025 모드에서 현재 페이지에 필요한 파일만 선택적으로 다운로드
 */

class SmartPrefetch {
    constructor() {
        this.isActive = false;
        this.currentCategory = null;
        this.currentSubcategory = null;
        this.currentResultType = null;
        this.prefetchAllowed = false;
        this.initialized = false;
        this.currentPrefetchPromise = null;
        this.activePrefetchKey = null;
        this.lastCompletedKey = null;
    }

    async init() {
        if (this.initialized) {
            return;
        }
        this.initialized = true;

        try {
            const response = await fetch('/api/prefetch/config');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const config = await response.json();
            if (!config.enabled) {
                console.log('🚫 2025 모드가 아니므로 스마트 프리페치 비활성화');
                this.prefetchAllowed = false;
                this.dispatchPrefetchEvent('smartPrefetch:ready', null);
                return;
            }

            this.prefetchAllowed = true;
            await this.prefetchCurrentPage();
            this.startAutoMode();
        } catch (error) {
            console.warn('⚠️ 스마트 프리페치 초기화 실패:', error);
            this.prefetchAllowed = false;
            this.dispatchPrefetchEvent('smartPrefetch:error', null, {
                message: error?.message || String(error)
            });
            this.dispatchPrefetchEvent('smartPrefetch:ready', null);
        }
    }

    dispatchPrefetchEvent(type, urlInfo, extra = {}) {
        const detail = {
            category: urlInfo?.category ?? null,
            subcategory: urlInfo?.subcategory ?? null,
            result_type: urlInfo?.result_type ?? null,
            ...extra
        };
        window.dispatchEvent(new CustomEvent(type, { detail }));
    }

    /**
     * URL에서 카테고리 정보 추출
     */
    parseCurrentUrl() {
        const path = window.location.pathname;

        // /search/dataA/safetykorea
        const dataAMatch = path.match(/^\/search\/(dataA)\/([^\/]+)$/);
        if (dataAMatch) {
            return {
                category: dataAMatch[1],
                subcategory: dataAMatch[2],
                result_type: null
            };
        }

        // /search/dataB/wadiz-makers
        const dataBMatch = path.match(/^\/search\/(dataB)\/([^\/]+)$/);
        if (dataBMatch) {
            return {
                category: dataBMatch[1],
                subcategory: dataBMatch[2],
                result_type: null
            };
        }

        // /search/dataC/success/safetykorea
        // /search/dataC/failed/safetykorea
        const dataCMatch = path.match(/^\/search\/(dataC)\/(success|failed)\/([^\/]+)$/);
        if (dataCMatch) {
            return {
                category: dataCMatch[1],
                subcategory: dataCMatch[3],
                result_type: dataCMatch[2]
            };
        }

        return null;
    }

    /**
     * 현재 페이지에 필요한 파일 다운로드
     */
    async prefetchCurrentPage(forcedUrlInfo = null) {
        if (!this.prefetchAllowed) {
            return;
        }

        const urlInfo = forcedUrlInfo || this.parseCurrentUrl();
        if (!urlInfo) {
            console.log('📄 검색 페이지가 아니므로 프리페치 스킵');
            return;
        }

        const key = this.buildPrefetchKey(urlInfo);

        if (this.currentPrefetchPromise && this.activePrefetchKey === key) {
            await this.currentPrefetchPromise;
            return;
        }

        if (this.lastCompletedKey === key && !this.currentPrefetchPromise) {
            console.log('✅ 이미 현재 페이지 파일이 캐시되어 있음');
            this.dispatchPrefetchEvent('smartPrefetch:ready', urlInfo);
            return;
        }

        this.currentPrefetchPromise = this.runPrefetchSequence(urlInfo, key);
        this.activePrefetchKey = key;
        await this.currentPrefetchPromise.finally(() => {
            if (this.activePrefetchKey === key) {
                this.activePrefetchKey = null;
            }
            this.currentPrefetchPromise = null;
        });
    }

    buildPrefetchKey({ category, subcategory, result_type }) {
        return [category || '', result_type || '', subcategory || ''].join('|');
    }

    async runPrefetchSequence(urlInfo, key) {
        this.isActive = true;
        console.log(`🎯 스마트 프리페치 시작: ${urlInfo.category}/${urlInfo.subcategory}${urlInfo.result_type ? '/' + urlInfo.result_type : ''}`);
        this.dispatchPrefetchEvent('smartPrefetch:start', urlInfo);

        try {
            const startTime = Date.now();

            const response = await fetch('/api/prefetch-single', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    category: urlInfo.category,
                    subcategory: urlInfo.subcategory,
                    result_type: urlInfo.result_type
                })
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));

                if (response.status === 400) {
                    console.log('🚫 서버에서 2025 모드 비활성화 응답 수신, 스마트 프리페치 중단');
                    this.prefetchAllowed = false;
                    this.dispatchPrefetchEvent('smartPrefetch:error', urlInfo, {
                        message: 'prefetch disabled by server'
                    });
                    this.dispatchPrefetchEvent('smartPrefetch:ready', urlInfo);
                    return;
                }

                throw new Error(errorData.detail || `HTTP ${response.status}`);
            }

            const result = await response.json();
            const endTime = Date.now();
            const totalDuration = (endTime - startTime) / 1000;

            // 성공 정보 표시
            this.displaySuccess(result, totalDuration);

            // 검색 워밍업 실행 (첫 검색 지연 완화)
            await this.warmupSearch(urlInfo);

            // 현재 상태 업데이트
            this.currentCategory = urlInfo.category;
            this.currentSubcategory = urlInfo.subcategory;
            this.currentResultType = urlInfo.result_type;
            this.lastCompletedKey = key;
            this.dispatchPrefetchEvent('smartPrefetch:ready', urlInfo);

        } catch (error) {
            console.error('❌ 스마트 프리페치 실패:', error);
            console.log('💡 R2 직접 모드로 fallback됩니다');
            this.lastCompletedKey = key;
            this.dispatchPrefetchEvent('smartPrefetch:error', urlInfo, {
                message: error?.message || String(error)
            });
            this.dispatchPrefetchEvent('smartPrefetch:ready', urlInfo);
        } finally {
            this.isActive = false;
        }
    }

    /**
     * 성공 정보 표시
     */
    displaySuccess(result, totalDuration) {
        const { stats } = result;

        console.log('%c🚀 스마트 프리페치 완료!',
            'color: #00ff00; font-size: 16px; font-weight: bold;');

        console.log('%c📊 다운로드 통계:',
            'color: #00bfff; font-size: 14px; font-weight: bold;');

        console.table({
            '카테고리': `${stats.category}${stats.result_type ? '/' + stats.result_type : ''}`,
            '서브카테고리': stats.subcategory,
            '로컬 경로': stats.local_path,
            '다운로드 시간': `${stats.duration_seconds}초`,
            '전체 소요 시간': `${totalDuration.toFixed(2)}초`,
            '캐시 정리': stats.cache_cleared ? '✅' : '❌'
        });

        // 성능 분석
        if (stats.duration_seconds < 2) {
            console.log('%c⚡ 우수: 다운로드 속도가 매우 빠릅니다!',
                'color: #00ff00; font-weight: bold;');
        } else if (stats.duration_seconds < 5) {
            console.log('%c👍 양호: 다운로드 속도가 적절합니다',
                'color: #ffff00; font-weight: bold;');
        } else {
            console.log('%c⚠️ 느림: 네트워크 상태를 확인해보세요',
                'color: #ff6600; font-weight: bold;');
        }

        console.log('%c💡 이제 이 페이지의 검색이 빠르게 실행됩니다!',
            'color: #ffa500; font-weight: bold;');
    }

    /**
     * 검색 워밍업용 최소 쿼리 실행 (첫 검색 지연 최소화)
     */
    async warmupSearch(urlInfo) {
        if (!urlInfo || !this.prefetchAllowed) {
            return;
        }

        const endpoint = this.buildSearchEndpoint(urlInfo);
        if (!endpoint) {
            return;
        }

        const payload = {
            keyword: '',
            search_field: 'product_name',
            page: 1,
            limit: 1,
            filters: null
        };

        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                throw new Error(`Warmup HTTP ${response.status}`);
            }

            await response.json();
            console.log('🔥 워밍업 검색 완료:', endpoint);
        } catch (error) {
            console.debug('⚠️ 워밍업 검색 건너뜀:', error.message);
        }
    }

    buildSearchEndpoint({ category, subcategory, result_type }) {
        if (!category || !subcategory) {
            return null;
        }

        const encodedSubcategory = encodeURIComponent(subcategory);

        if (category === 'dataC') {
            if (!result_type) {
                return null;
            }
            const encodedResultType = encodeURIComponent(result_type);
            return `/api/search/dataC/${encodedResultType}/${encodedSubcategory}`;
        }

        if (category === 'dataA') {
            return `/api/search/dataA/${encodedSubcategory}`;
        }

        if (category === 'dataB') {
            return `/api/search/dataB/${encodedSubcategory}`;
        }

        return `/api/search/${encodeURIComponent(category)}/${encodedSubcategory}`;
    }

    /**
     * 페이지 변경 감지 및 자동 프리페치
     */
    startAutoMode() {
        if (!this.prefetchAllowed) {
            return;
        }

        // popstate 이벤트 (뒤로가기/앞으로가기)
        window.addEventListener('popstate', () => {
            setTimeout(() => this.prefetchCurrentPage(), 100);
        });

        // pushState/replaceState 감지 (SPA 네비게이션)
        const originalPushState = history.pushState;
        const originalReplaceState = history.replaceState;

        history.pushState = function(...args) {
            originalPushState.apply(history, args);
            setTimeout(() => {
                if (window.smartPrefetch.prefetchAllowed) {
                    window.smartPrefetch.prefetchCurrentPage();
                }
            }, 100);
        };

        history.replaceState = function(...args) {
            originalReplaceState.apply(history, args);
            setTimeout(() => {
                if (window.smartPrefetch.prefetchAllowed) {
                    window.smartPrefetch.prefetchCurrentPage();
                }
            }, 100);
        };
    }

    /**
     * 수동 프리페치 실행
     */
    async manualPrefetch(category, subcategory, resultType = null) {
        console.log(`🔧 수동 프리페치: ${category}/${subcategory}${resultType ? '/' + resultType : ''}`);

        if (!this.prefetchAllowed) {
            console.log('⚠️ 스마트 프리페치 비활성화 상태입니다');
            return;
        }

        const urlInfo = { category, subcategory, result_type: resultType };
        await this.prefetchCurrentPage(urlInfo);
    }

    async waitUntilReady(info = null) {
        if (!this.prefetchAllowed) {
            return;
        }

        const urlInfo = info || this.parseCurrentUrl();
        if (!urlInfo) {
            return;
        }

        const key = this.buildPrefetchKey(urlInfo);

        if (this.lastCompletedKey === key && !this.currentPrefetchPromise) {
            return;
        }

        if (this.currentPrefetchPromise) {
            await this.currentPrefetchPromise;
            return;
        }

        await this.prefetchCurrentPage(urlInfo);
    }

    isReady(info = null) {
        if (!this.prefetchAllowed) {
            return true;
        }

        const urlInfo = info || this.parseCurrentUrl();
        if (!urlInfo) {
            return true;
        }

        const key = this.buildPrefetchKey(urlInfo);
        return !this.currentPrefetchPromise && this.lastCompletedKey === key;
    }
}

// 전역 인스턴스 생성
window.smartPrefetch = new SmartPrefetch();

// DOMContentLoaded 시 자동 시작
document.addEventListener('DOMContentLoaded', () => {
    console.log('🎯 스마트 프리페치 시스템 초기화...');
    window.smartPrefetch.init();
});

// BFCache 등으로 복귀했을 때도 즉시 프리페치 재시도
window.addEventListener('pageshow', () => {
    if (window.smartPrefetch && window.smartPrefetch.prefetchAllowed) {
        window.smartPrefetch.prefetchCurrentPage();
    }
});

// 개발자 편의 함수
window.manualPrefetch = (category, subcategory, resultType) => {
    return window.smartPrefetch.manualPrefetch(category, subcategory, resultType);
};
