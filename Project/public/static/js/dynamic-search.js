/**
 * 동적 검색 및 렌더링 엔진
 * 설정 파일 기반으로 동적으로 필드를 렌더링
 */

class DynamicSearchEngine {
    constructor() {
        this.currentCategory = null;
        this.currentSubcategory = null;
        this.currentResultType = null; // dataC용 추가 매개변수
        this.currentConfig = null;
        this.currentSearchResults = [];
        this.currentPage = 1;
        this.itemsPerPage = 20;
        this.isSearching = false;
        this.isLargeFile = false;

        // 검색 조건 저장용
        this.lastSearchKeyword = '';
        this.lastSearchField = 'product_name';

        // 다운로드 제어용 AbortController
        this.downloadAbortController = null;
        this.prefetchHandlersBound = false;
        this.prefetchHandlers = null;

        this.init();
    }

    formatToKST(date) {
        const offsetMinutes = date.getTimezoneOffset();
        const kstOffsetMinutes = -9 * 60; // KST (UTC+9)
        const diff = (kstOffsetMinutes - offsetMinutes) * 60000;
        const kstDate = new Date(date.getTime() + diff);

        const yyyy = kstDate.getFullYear();
        const mm = String(kstDate.getMonth() + 1).padStart(2, '0');
        const dd = String(kstDate.getDate()).padStart(2, '0');
        const HH = String(kstDate.getHours()).padStart(2, '0');
        const MM = String(kstDate.getMinutes()).padStart(2, '0');
        const SS = String(kstDate.getSeconds()).padStart(2, '0');
        const MS = String(kstDate.getMilliseconds()).padStart(3, '0');

        return `${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS}.${MS}`;
    }

    async init() {
        // URL에서 카테고리 정보 추출
        this.extractCategoryFromUrl();
        
        // 설정 로드
        await this.loadConfig();
        
        
        // 페이지 초기화
        this.initializePage();
        
        // 이벤트 리스너 설정
        this.setupEventListeners();
    }
    
    extractCategoryFromUrl() {
        const pathParts = window.location.pathname.split('/').filter(part => part !== '');
        console.log('URL path parts:', pathParts);
        console.log('Full URL:', window.location.href);
        
        // API 엔드포인트에 맞는 카테고리 매핑
        const categoryMapping = {
            // dataA 카테고리
            'safetykorea': ['dataA', 'safetykorea'],
            'kwtc': ['dataA', 'kwtc'],
            'rra-self-conformity': ['dataA', 'rra-self-conformity'],
            'rra-certification': ['dataA', 'rra-certification'],
            'efficiency-rating': ['dataA', 'efficiency-rating'],
            'high-efficiency': ['dataA', 'high-efficiency'],
            'standby-power': ['dataA', 'standby-power'],
            'approval-details': ['dataA', 'approval-details'],
            'declaration-details': ['dataA', 'declaration-details'],
            'recall': ['dataA', 'recall'],
            'safetykoreachild': ['dataA', 'safetykoreachild'],
            
            // dataB 카테고리
            'wadiz-makers': ['dataB', 'wadiz-makers']
        };
        
        // URL 패턴 처리
        if (pathParts.length >= 4 && pathParts[0] === 'search' && pathParts[1] === 'dataC') {
            // dataC 3-parameter 패턴: /search/dataC/success/safetykorea
            this.currentCategory = pathParts[1];  // 'dataC'
            this.currentResultType = pathParts[2];  // 'success' 또는 'failed'
            this.currentSubcategory = pathParts[3];  // 'safetykorea'
            
        } else if (pathParts.length >= 3 && pathParts[0] === 'search') {
            // dataA/dataB 2-parameter 패턴: /search/dataA/safetykorea
            this.currentCategory = pathParts[1];
            this.currentSubcategory = pathParts[2];
            this.currentResultType = null;
            
        } else if (pathParts.length >= 2) {
            // 마지막 경로 요소로 매핑 테이블에서 찾기 (기존 호환성)
            const lastPart = pathParts[pathParts.length-1];
            
            // 매핑 테이블에서 검색
            if (categoryMapping[lastPart]) {
                [this.currentCategory, this.currentSubcategory] = categoryMapping[lastPart];
                this.currentResultType = null;
            } else {
                // 기본값 사용
                this.currentCategory = 'dataA';
                this.currentSubcategory = 'safetykorea';
                this.currentResultType = null;
            }
        } else {
            // URL 구조를 파악할 수 없는 경우 기본값 사용
            console.warn('URL에서 카테고리 정보를 추출할 수 없습니다. 기본값을 사용합니다.');
            this.currentCategory = 'dataA';
            this.currentSubcategory = 'safetykorea';
            this.currentResultType = null;
        }
        
        console.log('추출된 카테고리:', {
            category: this.currentCategory,
            subcategory: this.currentSubcategory,
            resultType: this.currentResultType
        });
    }
    
    async loadConfig() {
        if (!this.currentCategory || !this.currentSubcategory) {
            console.error('카테고리 정보가 없습니다:', {
                category: this.currentCategory,
                subcategory: this.currentSubcategory,
                url: window.location.pathname
            });
            // 실제 존재하는 엔드포인트로 폴백
            this.currentCategory = this.currentCategory || 'dataA';
            this.currentSubcategory = this.currentSubcategory || 'safetykorea';
            console.log('기본 카테고리로 설정:', this.currentCategory, this.currentSubcategory);
        }
        
        try {
            // API URL 구성 (dataC는 3-parameter, 나머지는 2-parameter)
            let apiUrl;
            if (this.currentCategory === 'dataC' && this.currentResultType) {
                apiUrl = `/api/settings/${this.currentCategory}/${this.currentResultType}/${this.currentSubcategory}`;
            } else {
                apiUrl = `/api/settings/${this.currentCategory}/${this.currentSubcategory}`;
            }
            
            // 새로운 settings API를 우선 시도
            let response = await fetch(apiUrl);
            
            if (!response.ok) {
                // 폴백: 기존 config API 사용
                if (this.currentCategory === 'dataC' && this.currentResultType) {
                    response = await fetch(`/api/config/${this.currentCategory}/${this.currentResultType}/${this.currentSubcategory}`);
                } else {
                    response = await fetch(`/api/config/${this.currentCategory}/${this.currentSubcategory}`);
                }
            }
            
            if (response.ok) {
                this.currentConfig = await response.json();
                console.log('설정 로드 완료:', this.currentConfig);
            } else {
                console.error('설정 로드 실패:', response.status);
                // 기본 설정 사용
                this.currentConfig = this.getDefaultConfig();
            }
        } catch (error) {
            console.error('설정 로드 오류:', error);
            this.currentConfig = this.getDefaultConfig();
        }
    }
    
    getDefaultConfig() {
        return {
            displayName: "데이터 검색",
            description: "원하시는 키워드를 입력하여 데이터를 검색하세요.",
            displayFields: [
                { field: "title", name: "제목", width: "40%", type: "text" },
                { field: "content", name: "내용", width: "60%", type: "text" }
            ],
            searchFields: [
                { field: "all", name: "전체", placeholder: "전체 필드에서 검색" }
            ],
            pagination: { itemsPerPage: 20 }
        };
    }
    
    initializePage() {
        if (!this.currentConfig) return;

        // 페이지 제목 및 설명 설정
        document.getElementById('search-title').textContent = this.currentConfig.displayName;
        document.getElementById('search-description').textContent = this.currentConfig.description;
        
        // 검색 필드 옵션 설정
        this.setupSearchFields();
        
        // 페이지당 항목 수 설정
        this.itemsPerPage = this.currentConfig.pagination?.itemsPerPage || 20;

        // 설정 메뉴 제거됨

        this.setupPrefetchGuards();
    }

    getPrefetchContext() {
        return {
            category: this.currentCategory || null,
            subcategory: this.currentSubcategory || null,
            result_type: this.currentResultType || null
        };
    }

    setSearchButtonDisabled(disabled) {
        const searchButton = document.getElementById('search-btn');
        if (!searchButton) return;

        searchButton.disabled = disabled;
        if (disabled) {
            searchButton.dataset.prefetchLock = 'true';
        } else {
            delete searchButton.dataset.prefetchLock;
        }
    }

    isMatchingPrefetchDetail(detail) {
        if (!detail) {
            return true;
        }

        const context = this.getPrefetchContext();
        const normalize = (value) => value ?? null;

        return normalize(detail.category) === normalize(context.category) &&
               normalize(detail.subcategory) === normalize(context.subcategory) &&
               normalize(detail.result_type) === normalize(context.result_type);
    }

    setupPrefetchGuards() {
        const searchButton = document.getElementById('search-btn');
        const prefetch = window.smartPrefetch;

        if (!searchButton) {
            return;
        }

        // 기본적으로 워밍업 완료 전까지 버튼 비활성화
        this.setSearchButtonDisabled(true);

        if (!prefetch) {
            this.setSearchButtonDisabled(false);
            return;
        }

        if (!this.prefetchHandlersBound) {
            const handleStart = (event) => {
                if (this.isMatchingPrefetchDetail(event.detail)) {
                    this.setSearchButtonDisabled(true);
                }
            };

            const handleReady = (event) => {
                if (this.isMatchingPrefetchDetail(event.detail)) {
                    this.setSearchButtonDisabled(false);
                }
            };

            const handleError = (event) => {
                if (this.isMatchingPrefetchDetail(event.detail)) {
                    this.setSearchButtonDisabled(false);
                }
            };

            window.addEventListener('smartPrefetch:start', handleStart);
            window.addEventListener('smartPrefetch:ready', handleReady);
            window.addEventListener('smartPrefetch:error', handleError);

            this.prefetchHandlers = { handleStart, handleReady, handleError };
            this.prefetchHandlersBound = true;
        }

        if (!prefetch.initialized) {
            return;
        }

        if (!prefetch.prefetchAllowed) {
            this.setSearchButtonDisabled(false);
            return;
        }

        const context = this.getPrefetchContext();

        if (!prefetch.isReady(context)) {
            this.setSearchButtonDisabled(true);
        } else {
            this.setSearchButtonDisabled(false);
        }
    }
    
    setupSearchFields() {
        const searchFieldSelect = document.getElementById('search-field');
        if (!searchFieldSelect || !this.currentConfig.searchFields) return;
        
        searchFieldSelect.innerHTML = this.currentConfig.searchFields.map(field => 
            `<option value="${field.field}">${field.name}</option>`
        ).join('');
        
        // 플레이스홀더 동적 변경
        const searchInput = document.getElementById('search-input');
        searchFieldSelect.addEventListener('change', () => {
            const selectedField = this.currentConfig.searchFields.find(
                f => f.field === searchFieldSelect.value
            );
            if (selectedField && selectedField.placeholder) {
                searchInput.placeholder = selectedField.placeholder;
            }
        });
        
        // 초기 플레이스홀더 설정
        if (this.currentConfig.searchFields[0]?.placeholder) {
            searchInput.placeholder = this.currentConfig.searchFields[0].placeholder;
        }
    }
    
    
    
    
    clearSampleMode() {
        // 샘플 모드 안내 제거
        const sampleInfo = document.getElementById('sample-mode-info');
        if (sampleInfo) {
            sampleInfo.remove();
        }
    }
    
    // 설정 메뉴 제거됨
    
    setupEventListeners() {
        // 검색 버튼 클릭
        document.getElementById('search-btn').addEventListener('click', () => {
            this.performSearch();
        });
        
        // 엔터키 검색
        document.getElementById('search-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.performSearch();
            }
        });
        
        // 전체 다운로드 버튼
        document.getElementById('download-all-btn').addEventListener('click', () => {
            this.showDownloadModal();
        });
    }
    
    async performSearch(keywordParam = null, searchFieldParam = null) {
        const ready = await this.ensurePrefetchReady();
        if (!ready) {
            return;
        }

        if (this.isSearching) return;

        const keyword = keywordParam || document.getElementById('search-input').value.trim();
        const searchField = searchFieldParam || document.getElementById('search-field').value;
        
        if (!keyword) {
            this.showMessage('검색어를 입력해주세요.', 'warning');
            return;
        }

        if (keyword.length < 2) {
            this.showMessage('검색어는 2글자 이상 입력해주세요.', 'warning');
            return;
        }

        // 인증번호 필드 별도 검증 (나중에 활성화 가능)
        const certNoValidation = this.validateCertNoSearch(keyword, searchField);
        if (!certNoValidation.isValid) {
            this.showMessage(certNoValidation.message, 'warning');
            return;
        }

        // 새로운 검색인 경우 페이지를 1로 초기화
        if (!keywordParam) {
            this.currentPage = 1;
        }

        // 샘플 모드 정리
        this.clearSampleMode();
        
        this.isSearching = true;
        this.showLoading(true);
        
        try {
            // API URL 구성 (dataC는 3-parameter, 나머지는 2-parameter)
            let searchUrl;
            if (this.currentCategory === 'dataC' && this.currentResultType) {
                searchUrl = `/api/search/${this.currentCategory}/${this.currentResultType}/${this.currentSubcategory}`;
            } else {
                searchUrl = `/api/search/${this.currentCategory}/${this.currentSubcategory}`;
            }
            
            // 성능 측정 시작
            const startTime = performance.now();
            const startDate = new Date();

            const response = await fetch(searchUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    keyword: keyword,
                    search_field: searchField,
                    page: this.currentPage || 1,  // 현재 페이지 (서버사이드 페이지네이션)
                    limit: this.itemsPerPage || 20  // 설정 기반 페이지당 항목 수
                })
            });

            // 성능 측정 완료
            const endTime = performance.now();
            const endDate = new Date();
            const duration = endTime - startTime;

            if (response.ok) {
                const data = await response.json();
                this.currentSearchResults = data.results || [];
                this.paginationInfo = data.pagination || {};
                this.currentPage = this.paginationInfo.current_page || 1;

                // 성능 측정 결과 콘솔 출력
                console.log(`🚀 API 성능 측정:`);
                console.log(`   시작 시간 (KST): ${this.formatToKST(startDate)}`);
                console.log(`   완료 시간 (KST): ${this.formatToKST(endDate)}`);
                console.log(`   총 처리시간: ${duration.toFixed(1)}ms`);

                // 검색 조건 저장 (다운로드용)
                this.lastSearchKeyword = keyword;
                this.lastSearchField = searchField;

                this.renderResults();
                this.updateResultsInfo();
                
            } else {
                const error = await response.json();
                this.showMessage(`검색 실패: ${error.detail}`, 'error');
            }
            
        } catch (error) {
            console.error('검색 오류:', error);
            this.showMessage('검색 중 오류가 발생했습니다.', 'error');
        } finally {
            this.isSearching = false;
            this.showLoading(false);
        }
    }

    async ensurePrefetchReady() {
        const prefetch = window.smartPrefetch;
        if (!prefetch || !prefetch.prefetchAllowed) {
            return true;
        }

        const context = this.getPrefetchContext();

        if (prefetch.isReady(context)) {
            return true;
        }

        this.setSearchButtonDisabled(true);

        // 프리페치가 진행 중인지 확인 후 없으면 시작
        if (!prefetch.isReady(context)) {
            prefetch.prefetchCurrentPage(context);
        }

        return false;
    }
    
    renderResults() {
        const resultsContainer = document.getElementById('search-results');
        const noResultsDiv = document.getElementById('no-results');
        const initialMessage = document.getElementById('initial-message');
        
        // 초기 메시지 숨기기
        initialMessage.style.display = 'none';
        
        if (this.currentSearchResults.length === 0) {
            resultsContainer.style.display = 'none';
            noResultsDiv.style.display = 'block';
            return;
        }
        
        noResultsDiv.style.display = 'none';
        resultsContainer.style.display = 'block';

        // 서버사이드 페이지네이션: 모든 결과를 렌더링 (서버에서 이미 페이징된 데이터)
        resultsContainer.innerHTML = this.renderResultsWithConfig(this.currentSearchResults);
        
        // 페이지네이션 생성
        this.createPagination();
    }
    
    renderResultsWithConfig(results) {
        if (!this.currentConfig || !this.currentConfig.displayFields) {
            return this.renderResultsDefault(results);
        }
        
        return `
            <div class="results-table-container">
                <table class="results-table">
                    <thead>
                        <tr>
                            ${this.currentConfig.displayFields.map(field => 
                                `<th style="width: ${field.width}; text-align: ${field.align || 'left'}">${field.name}</th>`
                            ).join('')}
                        </tr>
                    </thead>
                    <tbody>
                        ${results.map(item => this.renderResultRow(item)).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }
    
    renderResultRow(item) {
        // resultData 구조 처리
        const data = item.resultData || item;
        
        return `
            <tr class="result-row">
                ${this.currentConfig.displayFields.map(field => {
                    const value = this.getFieldValue(data, field);
                    const formattedValue = this.formatFieldValue(value, field);
                    const dataType = field.type || 'text';
                    return `<td data-type="${dataType}" style="text-align: ${field.align || 'left'}">${formattedValue}</td>`;
                }).join('')}
            </tr>
        `;
    }
    
    getFieldValue(data, field) {
        const value = data[field.field];
        if (value === null || value === undefined) return '';
        return value;
    }
    
    formatFieldValue(value, field) {
        if (!value) return '-';
        
        // 디버깅: 이미지 필드 처리 확인
        if (field.field === 'item' || field.type === 'image') {
            console.log('Processing image field:', {
                field: field.field,
                type: field.type,
                value: value,
                isURL: value && (value.startsWith('http') || value.startsWith('/'))
            });
        }
        
        let formattedValue;
        
        switch (field.type) {
            case 'date':
                formattedValue = this.formatDate(value);
                break;
            case 'number':
                formattedValue = this.formatNumber(value);
                break;
            case 'array':
                if (Array.isArray(value)) {
                    if (value.length === 0) {
                        formattedValue = '-';
                    } else {
                        // 이미지 URL 배열인 경우 특별 처리
                        if (field.field.includes('Image') || field.field.includes('image')) {
                            formattedValue = value.map(url => 
                                `<img src="${url}" alt="인증이미지" style="width: 40px; height: 40px; object-fit: cover; border-radius: 3px; margin: 1px; cursor: pointer; border: 1px solid #ddd;" onclick="window.open('${url}', '_blank')" onerror="this.style.display='none';" />`
                            ).join('');
                            if (formattedValue === '') formattedValue = '<span style="font-size: 12px; color: #999;">이미지 없음</span>';
                        } else {
                            // 객체 배열인 경우 의미있는 정보 추출
                            if (value.length > 0 && typeof value[0] === 'object' && value[0] !== null) {
                                if (field.field === 'factories') {
                                    // 공장 정보: 제조업체명을 표시
                                    formattedValue = value.map(factory => factory.makerName || '제조업체명 없음').join(', ');
                                } else if (field.field === 'similarCertifications') {
                                    // 유사인증: 인증번호만 표시
                                    formattedValue = value.map(cert => cert.certNum || '인증번호없음').join(', ');
                                } else {
                                    // 기타 객체 배열: 첫 번째 비어있지 않은 값들을 표시
                                    const obj = value[0];
                                    const meaningfulValues = Object.keys(obj)
                                        .filter(key => obj[key] && obj[key] !== '')
                                        .slice(0, 2)  // 최대 2개 필드만
                                        .map(key => obj[key]);
                                    formattedValue = meaningfulValues.length > 0 ? meaningfulValues.join(', ') : '정보 없음';
                                }
                            } else {
                                // 일반 배열은 문자열로 표시
                                formattedValue = value.join(', ');
                            }
                        }
                    }
                } else {
                    formattedValue = value || '-';
                }
                break;
            case 'link':
                formattedValue = `<a href="${value}" target="_blank">${this.truncateText(value, 50)}</a>`;
                break;
            case 'image':
                if (value && (value.startsWith('http') || value.startsWith('/'))) {
                    formattedValue = `<img src="${value}" alt="제품이미지" style="width: 60px; height: 60px; object-fit: cover; border-radius: 4px; cursor: pointer; border: 1px solid #ddd;" onclick="window.open('${value}', '_blank')" onerror="this.style.display='none'; this.nextSibling.style.display='inline';" /><span style="display: none; font-size: 12px; color: #666;">이미지 없음</span>`;
                } else {
                    formattedValue = '<span style="font-size: 12px; color: #999;">-</span>';
                }
                break;
            default:
                // 이미지 URL이 포함된 필드 자동 감지 및 처리
                const stringValue = String(value);
                if (this.isImageField(field.field) || this.containsImageUrl(stringValue)) {
                    // 이미지 URL 처리
                    if (stringValue && (stringValue.startsWith('http') || stringValue.startsWith('/'))) {
                        formattedValue = `<img src="${stringValue}" alt="이미지" style="width: 60px; height: 60px; object-fit: cover; border-radius: 4px; cursor: pointer; border: 1px solid #ddd;" onclick="window.open('${stringValue}', '_blank')" onerror="this.style.display='none'; this.nextSibling.style.display='inline';" /><span style="display: none; font-size: 12px; color: #666;">이미지 없음</span>`;
                    } else {
                        formattedValue = '<span style="font-size: 12px; color: #999;">-</span>';
                    }
                } else if (stringValue.includes('&gt;') || stringValue.includes('&lt;') || stringValue.includes('&amp;')) {
                    // HTML 엔티티가 포함된 텍스트를 올바르게 디코딩
                    formattedValue = this.unescapeHtml(stringValue);
                } else {
                    formattedValue = this.escapeHtml(stringValue);
                }
                break;
        }
        
        // 텍스트 길이 제한 적용 (필드 타입과 너비에 따라 차등 적용)
        const isImageContent = this.isImageField(field.field) || this.containsImageUrl(String(value));
        if (field.type !== 'link' && field.type !== 'date' && field.type !== 'number' && field.type !== 'image' && !isImageContent) {
            let maxLength = 100; // 기본값
            
            // 필드 너비에 따른 길이 조정
            if (field.width) {
                const widthPercent = parseInt(field.width.replace('%', ''));
                if (widthPercent <= 8) maxLength = 30;
                else if (widthPercent <= 12) maxLength = 50;
                else if (widthPercent <= 15) maxLength = 70;
                else if (widthPercent <= 20) maxLength = 100;
                else maxLength = 150;
            }
            
            formattedValue = this.truncateText(formattedValue, maxLength);
        }
        
        return formattedValue;
    }
    
    formatDate(dateStr) {
        if (!dateStr) return '-';
        
        // YYYYMMDD 형식을 YYYY-MM-DD로 변환
        if (/^\d{8}$/.test(dateStr)) {
            return `${dateStr.slice(0,4)}-${dateStr.slice(4,6)}-${dateStr.slice(6,8)}`;
        }
        
        return dateStr;
    }
    
    formatNumber(value) {
        const num = parseFloat(value);
        return isNaN(num) ? value : num.toLocaleString();
    }
    
    truncateText(text, maxLength = 100) {
        if (!text) return text;
        const str = String(text);
        
        // 모든 텍스트에 1줄 제한 적용 (시각적으로 1줄까지만)
        return `<div class="text-clamp-1">${str}</div>`;
    }
    
    renderResultsDefault(results) {
        // 기본 렌더링 (설정이 없는 경우)
        return results.map(item => {
            const data = item.resultData || item;
            const keys = Object.keys(data).slice(0, 5); // 처음 5개 필드만 표시
            
            return `
                <div class="result-item">
                    ${keys.map(key => `
                        <div class="result-field">
                            <strong>${key}:</strong> ${this.escapeHtml(String(data[key] || ''))}
                        </div>
                    `).join('')}
                </div>
            `;
        }).join('');
    }
    
    createPagination() {
        const paginationContainer = document.getElementById('pagination-container');
        const totalPages = this.paginationInfo?.total_pages || 1;

        if (totalPages <= 1) {
            paginationContainer.style.display = 'none';
            return;
        }
        
        paginationContainer.style.display = 'flex';
        
        let paginationHTML = '';
        
        // 처음 페이지 버튼 (항상 표시, 1페이지이거나 첫 페이지일 때 비활성화)
        const firstDisabled = this.currentPage === 1 || totalPages <= 1;
        paginationHTML += `<button class="page-btn first-btn ${firstDisabled ? 'disabled' : ''}" ${firstDisabled ? 'disabled' : `onclick="searchEngine.goToPage(1)"`} title="첫 페이지로">≪ 처음</button>`;
        
        // 이전 페이지 버튼 (항상 표시, 첫 페이지이거나 1페이지만 있을 때 비활성화)
        const prevDisabled = this.currentPage <= 1 || totalPages <= 1;
        paginationHTML += `<button class="page-btn prev-btn ${prevDisabled ? 'disabled' : ''}" ${prevDisabled ? 'disabled' : `onclick="searchEngine.goToPage(${this.currentPage - 1})"`} title="이전 페이지">‹ 이전</button>`;
        
        // 페이지 번호들
        const maxVisiblePages = 5; // 보이는 페이지 번호 수
        let startPage, endPage;
        
        if (totalPages <= maxVisiblePages) {
            startPage = 1;
            endPage = totalPages;
        } else {
            const halfVisible = Math.floor(maxVisiblePages / 2);
            
            if (this.currentPage <= halfVisible) {
                startPage = 1;
                endPage = maxVisiblePages;
            } else if (this.currentPage + halfVisible >= totalPages) {
                startPage = totalPages - maxVisiblePages + 1;
                endPage = totalPages;
            } else {
                startPage = this.currentPage - halfVisible;
                endPage = this.currentPage + halfVisible;
            }
        }
        
        // 시작 페이지가 1보다 클 때 생략 표시
        if (startPage > 1) {
            paginationHTML += `<button class="page-btn" onclick="searchEngine.goToPage(1)">1</button>`;
            if (startPage > 2) {
                paginationHTML += `<span class="page-ellipsis">...</span>`;
            }
        }
        
        // 페이지 번호 버튼들
        for (let page = startPage; page <= endPage; page++) {
            const isActive = page === this.currentPage ? 'active' : '';
            paginationHTML += `<button class="page-btn ${isActive}" onclick="searchEngine.goToPage(${page})">${page}</button>`;
        }
        
        // 끝 페이지가 전체보다 작을 때 생략 표시
        if (endPage < totalPages) {
            if (endPage < totalPages - 1) {
                paginationHTML += `<span class="page-ellipsis">...</span>`;
            }
            paginationHTML += `<button class="page-btn" onclick="searchEngine.goToPage(${totalPages})">${totalPages}</button>`;
        }
        
        // 다음 페이지 버튼 (항상 표시, 마지막 페이지이거나 1페이지만 있을 때 비활성화)
        const nextDisabled = this.currentPage >= totalPages || totalPages <= 1;
        paginationHTML += `<button class="page-btn next-btn ${nextDisabled ? 'disabled' : ''}" ${nextDisabled ? 'disabled' : `onclick="searchEngine.goToPage(${this.currentPage + 1})"`} title="다음 페이지">다음 ›</button>`;
        
        // 끝 페이지 버튼 (항상 표시, 마지막 페이지이거나 1페이지만 있을 때 비활성화)
        const lastDisabled = this.currentPage === totalPages || totalPages <= 1;
        paginationHTML += `<button class="page-btn last-btn ${lastDisabled ? 'disabled' : ''}" ${lastDisabled ? 'disabled' : `onclick="searchEngine.goToPage(${totalPages})"`} title="마지막 페이지로">끝 ≫</button>`;
        
        // 페이지 정보 표시
        paginationHTML += `<span class="pagination-info">${this.currentPage} / ${totalPages} 페이지</span>`;
        
        paginationContainer.innerHTML = paginationHTML;
    }
    
    async goToPage(page) {
        this.currentPage = page;

        // 서버사이드 페이지네이션: 새로운 페이지 데이터 요청
        if (this.lastSearchKeyword !== undefined && this.lastSearchField !== undefined) {
            await this.performSearch(this.lastSearchKeyword, this.lastSearchField);
        }

        // 페이지 상단으로 스크롤
        document.querySelector('.search-section').scrollIntoView({
            behavior: 'smooth'
        });
    }
    
    updateResultsInfo() {
        const resultsInfo = document.getElementById('results-info');
        const totalCount = document.getElementById('total-count');

        if (this.currentSearchResults.length > 0) {
            const totalItems = this.paginationInfo?.total_count || this.currentSearchResults.length;
            totalCount.textContent = totalItems.toLocaleString();
            resultsInfo.style.display = 'flex';
        } else {
            resultsInfo.style.display = 'none';
        }
    }
    
    showDownloadModal() {
        if (!this.lastSearchKeyword) {
            this.showMessage('검색을 먼저 실행해주세요.', 'warning');
            return;
        }
        
        // 다운로드 모달 표시 (전체 건수 표시는 검색 후 업데이트)
        document.getElementById('download-modal').style.display = 'flex';
        document.getElementById('download-count').textContent = '전체 검색 결과';

        const progressDiv = document.getElementById('download-progress');
        if (progressDiv) {
            progressDiv.style.display = 'none';
        }
    }
    
    async startDownload() {
        const modal = document.getElementById('download-modal');
        const progressDiv = document.getElementById('download-progress');
        const progressMessage = document.getElementById('progress-message');
        const progressFill = document.getElementById('progress-fill');
        const progressPercentage = document.getElementById('progress-percentage');

        if (progressDiv) {
            progressDiv.style.display = 'block';
        }
        if (progressMessage) {
            progressMessage.innerHTML = '<span class="download-spinner"></span><span class="loading-dots">Excel 파일 생성 중</span>';
        }
        if (progressFill) {
            progressFill.style.width = '100%';
            progressFill.style.animation = 'progress-slide 1s linear infinite';
        }
        if (progressPercentage) {
            progressPercentage.style.display = 'none';
        }

        if (this.downloadAbortController) {
            this.downloadAbortController.abort();
        }

        this.downloadAbortController = new AbortController();
        const { signal } = this.downloadAbortController;

        try {
            let downloadUrl;
            if (this.currentCategory === 'dataC' && this.currentResultType) {
                downloadUrl = `/api/download-search/${this.currentCategory}/${this.currentResultType}/${this.currentSubcategory}`;
            } else {
                downloadUrl = `/api/download-search/${this.currentCategory}/${this.currentSubcategory}`;
            }

            const response = await fetch(downloadUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    keyword: this.lastSearchKeyword,
                    search_field: this.lastSearchField,
                    file_format: 'xlsx'
                }),
                signal
            });

            if (!response.ok) {
                let errorDetail = '다운로드 요청이 실패했습니다.';
                try {
                    const errorBody = await response.json();
                    if (errorBody?.detail) {
                        errorDetail = errorBody.detail;
                    }
                } catch (jsonError) {
                    console.warn('다운로드 실패 응답 파싱 오류:', jsonError);
                }
                throw new Error(errorDetail);
            }

            const blob = await response.blob();
            const contentDisposition = response.headers.get('Content-Disposition');
            let filename = `datapage_export_${new Date().toISOString().replace(/[-:T]/g, '').slice(0, 14)}.xlsx`;

            if (contentDisposition) {
                const match = contentDisposition.match(/filename\*=UTF-8''(.+)$|filename="?([^";]+)"?/i);
                if (match) {
                    const encoded = match[1] || match[2];
                    if (encoded) {
                        try {
                            filename = decodeURIComponent(encoded);
                        } catch (decodeError) {
                            console.warn('파일명 디코딩 실패:', decodeError);
                            filename = encoded;
                        }
                    }
                }
            }

            const blobUrl = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = blobUrl;
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(blobUrl);

            this.showMessage('다운로드가 시작되었습니다.', 'info');
            modal.style.display = 'none';
        } catch (error) {
            if (error.name === 'AbortError') {
                this.showMessage('다운로드가 취소되었습니다.', 'warning');
            } else {
                console.error('다운로드 오류:', error);
                this.showMessage(`다운로드 중 오류가 발생했습니다. (${error.message || error})`, 'error');
            }
        } finally {
            if (progressDiv) {
                progressDiv.style.display = 'none';
            }
            this.downloadAbortController = null;
        }
    }
    getUserSession() {
        // 세션 스토리지에서 사용자 세션 ID 가져오기 또는 생성
        let sessionId = sessionStorage.getItem('datapage_session');
        if (!sessionId) {
            sessionId = 'session_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now();
            sessionStorage.setItem('datapage_session', sessionId);
        }
        return sessionId;
    }
    
    showLoading(show) {
        const searchBtn = document.getElementById('search-btn');
        if (show) {
            searchBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
            searchBtn.disabled = true;
        } else {
            searchBtn.innerHTML = '<i class="fas fa-search"></i>';
            searchBtn.disabled = false;
        }
    }
    
    showMessage(message, type = 'info') {
        // 간단한 토스트 메시지 (필요에 따라 확장 가능)
        console.log(`${type.toUpperCase()}: ${message}`);
        alert(message); // 임시 구현
    }
    
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
    // HTML 엔티티를 디코딩하는 함수
    unescapeHtml(text) {
        const div = document.createElement('div');
        div.innerHTML = text;
        return div.textContent || div.innerText || '';
    }
    
    // 이미지 필드인지 확인하는 함수
    isImageField(fieldName) {
        const imageFieldNames = [
            'certification_image_urls',
            'certification_image',
            '제품이미지',
            'image',
            'image_url',
            'img',
            'photo',
            'picture',
            'thumbnail'
        ];
        return imageFieldNames.some(name => 
            fieldName.toLowerCase().includes(name.toLowerCase())
        );
    }
    
    // URL이 이미지 URL인지 확인하는 함수
    containsImageUrl(value) {
        if (!value || typeof value !== 'string') return false;
        
        const imageExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'];
        const lowerValue = value.toLowerCase();
        
        return imageExtensions.some(ext => lowerValue.includes(ext)) ||
               (lowerValue.startsWith('http') && lowerValue.includes('image'));
    }
}

// 전역 인스턴스 생성
let searchEngine;

// 페이지 로드 완료 후 초기화
document.addEventListener('DOMContentLoaded', function() {
    searchEngine = new DynamicSearchEngine();
    
    // 모달 닫기 이벤트
    document.getElementById('modal-close').addEventListener('click', () => {
        if (searchEngine.downloadAbortController) {
            searchEngine.downloadAbortController.abort();
            searchEngine.downloadAbortController = null;
        }
        document.getElementById('download-modal').style.display = 'none';
    });
    
    // 취소 버튼 이벤트
    document.getElementById('cancel-download').addEventListener('click', () => {
        if (searchEngine.downloadAbortController) {
            searchEngine.downloadAbortController.abort();
            searchEngine.downloadAbortController = null;
        }
        document.getElementById('download-modal').style.display = 'none';
    });
    
    // 다운로드 시작 버튼 이벤트
    document.getElementById('start-download').addEventListener('click', () => {
        searchEngine.startDownload();
    });
    
    // 모달 외부 클릭시 닫기
    document.getElementById('download-modal').addEventListener('click', (e) => {
        if (e.target.id === 'download-modal') {
            if (searchEngine.downloadAbortController) {
                searchEngine.downloadAbortController.abort();
                searchEngine.downloadAbortController = null;
            }
            document.getElementById('download-modal').style.display = 'none';
        }
    });
});
