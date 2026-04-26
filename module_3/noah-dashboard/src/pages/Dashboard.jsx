import React, { useState } from 'react';
import { useReport } from '../hooks/useReport';
import KPICard from '../components/KPICard';
import CustomerBars from '../components/CustomerBars';
import StatusChart from '../components/StatusChart';
import OrderTable from '../components/OrderTable';
import AIInsight from '../components/allInsight';

function fmt(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + ' tỷ ₫';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + ' triệu ₫';
  return n.toLocaleString('vi-VN') + ' ₫';
}

export default function Dashboard() {
  const [page, setPage] = useState(1);
  const PAGE_SIZE = 10;
  const { data, loading, error, refresh } = useReport(page, PAGE_SIZE);

  const summary  = (data && data.summary) || {};
  const orders   = (data && data.orders) || [];
  const customers = (data && data.revenue_by_customer) || [];
  const pagination = (data && data.pagination) || { page: 1, page_size: PAGE_SIZE, total_rows: 0 };
  const paidRevenue = summary.total_revenue || 0;

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto', padding: '24px 32px' }}>

      {/* Modern Topbar */}
      <div style={{ 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'space-between', 
        marginBottom: 32,
        paddingBottom: 24,
        borderBottom: '1px solid var(--border)',
        flexWrap: 'wrap',
        gap: 20
      }} className="animate-fade-in">
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          <div style={{ 
            width: 48, height: 48, borderRadius: 14, 
            background: 'linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)', 
            display: 'flex', alignItems: 'center', justifyContent: 'center', 
            fontWeight: 700, fontSize: 20, color: '#fff',
            boxShadow: '0 8px 16px -4px rgba(37, 99, 235, 0.4)'
          }}>N</div>
          <div>
            <h1 style={{ fontSize: 24, fontWeight: 700, letterSpacing: '-0.5px' }}>Bảng Điều Khiển Tổng Quan</h1>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 4 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#10b981', boxShadow: '0 0 8px #10b981' }}></span>
              <p style={{ fontSize: 13, color: 'var(--muted)', fontWeight: 400 }}>Góc nhìn toàn diện kết hợp dữ liệu từ MySQL + PostgreSQL</p>
            </div>
          </div>
        </div>
        
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ 
            background: 'var(--surface2)', 
            padding: '8px 16px', 
            borderRadius: 10, 
            border: '1px solid var(--border)',
            display: 'flex',
            alignItems: 'center',
            gap: 10
          }}>
            <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--primary)', fontFamily: 'var(--mono)' }}>API STATUS</span>
            <span style={{ fontSize: 13, fontWeight: 500, color: 'var(--text)' }}>/api/report</span>
            <span style={{ padding: '2px 6px', borderRadius: 4, background: 'rgba(16, 185, 129, 0.1)', color: '#10b981', fontSize: 10, fontWeight: 700 }}>200 OK</span>
          </div>
          
          <button 
            onClick={refresh} 
            disabled={loading}
            className="glass-card"
            style={{ 
              background: loading ? 'var(--surface2)' : 'var(--primary)', 
              color: '#fff', 
              padding: '10px 24px', 
              borderRadius: 12, 
              border: 'none',
              cursor: loading ? 'not-allowed' : 'pointer', 
              fontSize: 14, 
              fontWeight: 600,
              boxShadow: loading ? 'none' : '0 10px 20px -5px rgba(59, 130, 246, 0.5)'
            }}>
            {loading ? 'Đang tải...' : 'Làm mới dữ liệu'}
          </button>
        </div>
      </div>

      {/* Error Message */}
      {error && (
        <div style={{ 
          background: 'rgba(239, 68, 68, 0.1)', 
          border: '1px solid rgba(239, 68, 68, 0.2)', 
          color: 'var(--danger)', 
          padding: '16px 20px', 
          borderRadius: 12, 
          marginBottom: 32, 
          fontSize: 14, 
          display: 'flex',
          alignItems: 'center',
          gap: 12
        }} className="animate-fade-in">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
          Lỗi: {error}
        </div>
      )}

      {/* KPI Grid */}
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', 
        gap: 24, 
        marginBottom: 32 
      }}>
        <KPICard label="Tổng đơn hàng"  value={summary.total_orders != null ? summary.total_orders.toLocaleString('vi-VN') : '0'} sub="Hệ thống MySQL" accent="blue" />
        <KPICard label="Đã thanh toán"  value={summary.paid_orders != null ? summary.paid_orders.toLocaleString('vi-VN') : '0'} sub="Cơ sở dữ liệu PostgreSQL" accent="green" />
        <KPICard label="Cần đối soát"   value={summary.pending_orders != null ? summary.pending_orders.toLocaleString('vi-VN') : '0'} sub="Chưa hoàn tất thanh toán" accent="amber" />
        <KPICard label="Doanh thu thực" value={fmt(paidRevenue)} sub="Tổng doanh thu hợp nhất" accent="purple" />
      </div>

      {/* Charts row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(450px, 1fr))', gap: 24, marginBottom: 32 }}>
        <div className="glass-card animate-fade-in" style={{ padding: 24 }}>
          <div style={{ marginBottom: 20 }}>
            <h3 style={{ fontSize: 16, fontWeight: 600 }}>Doanh thu theo khách hàng</h3>
            <p style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>Top 8 tài khoản có doanh thu cao nhất</p>
          </div>
          <CustomerBars customers={customers} />
        </div>
        <div className="glass-card animate-fade-in" style={{ padding: 24 }}>
          <div style={{ marginBottom: 20 }}>
            <h3 style={{ fontSize: 16, fontWeight: 600 }}>Cân đối thanh toán</h3>
            <p style={{ fontSize: 12, color: 'var(--muted)', marginTop: 2 }}>Tỷ lệ đơn hàng đã trả và đang chờ</p>
          </div>
          <div style={{ height: 260, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
            <StatusChart 
              paid={Number(summary.paid_orders ?? 0)} 
              pending={Number(summary.pending_orders ?? 0)} 
            />
          </div>
        </div>
      </div>

      <AIInsight />

      {/* Order Table Section */}
      <div className="glass-card animate-fade-in" style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ padding: '24px 24px 16px 24px', borderBottom: '1px solid var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div>
            <h3 style={{ fontSize: 18, fontWeight: 600 }}>Dòng dữ liệu đã hợp nhất</h3>
            <p style={{ fontSize: 13, color: 'var(--muted)', marginTop: 4 }}>Kết hợp dữ liệu Real-time từ MySQL và PostgreSQL</p>
          </div>
          <div style={{ fontSize: 12, color: 'var(--primary)', background: 'var(--primary-glow)', padding: '6px 12px', borderRadius: 8, fontWeight: 600, fontFamily: 'var(--mono)' }}>
            STITCHED DATA SOURCE
          </div>
        </div>
        <div style={{ padding: '0 8px' }}>
          <OrderTable
            orders={orders}
            page={pagination.page}
            pageSize={pagination.page_size}
            totalRows={pagination.total_rows}
            onPageChange={(p) => setPage(p)}
          />
        </div>
      </div>

      {/* Footer */}
      <div style={{ marginTop: 40, textAlign: 'center', paddingBottom: 40 }}>
        <p style={{ fontSize: 12, color: 'var(--muted)', letterSpacing: '0.5px' }}>
          © 2026 NOAH DATA PLATFORM · MODULE 3 ETL ANALYSIS
        </p>
      </div>
    </div>
  );
}
