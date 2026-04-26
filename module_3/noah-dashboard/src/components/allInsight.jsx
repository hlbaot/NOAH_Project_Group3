import { useState } from 'react';

export default function AIInsight() {
  const [insight, setInsight] = useState('');
  const [loading, setLoading] = useState(false);

  async function fetchInsight() {
    setLoading(true);
    setInsight('Đang gửi dữ liệu đã hợp nhất sang AI để phân tích...');
    try {
      const res = await fetch('/api/ai-insight');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setInsight(data.insight || 'AI chưa trả về nội dung phân tích.');
    } catch {
      setInsight('Không thể kết nối AI. Vui lòng thử lại.');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{
      background: 'var(--surface)', border: '1px solid var(--border)',
      borderRadius: 10, padding: 18, marginBottom: 20,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
        <div style={{ fontSize: 11, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontFamily: 'var(--mono)' }}>
          🤖 AI Insight — Phân tích doanh thu
        </div>
        <button type="button" onClick={fetchInsight} disabled={loading} aria-busy={loading} style={{
          background: '#1a2540', border: '1px solid #1e3a5f',
          color: '#22d3ee', padding: '5px 14px', borderRadius: 6,
          cursor: loading ? 'wait' : 'pointer', fontSize: 12,
          fontFamily: 'var(--mono)',
          opacity: loading ? 0.7 : 1,
        }}>
          {loading ? 'Đang phân tích...' : '✨ Phân tích ngay'}
        </button>
      </div>

      {insight && (
        <div
          style={{ fontSize: 14, color: 'var(--text)', lineHeight: 1.7, borderLeft: '3px solid #3b82f6', paddingLeft: 12 }}
          dangerouslySetInnerHTML={{ __html: insight.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>').replace(/\n/g, '<br/>') }}
        />
      )}

      {!insight && !loading && (
        <p style={{ fontSize: 13, color: 'var(--muted)', fontStyle: 'italic' }}>
          Nhấn "Phân tích ngay" để nhận nhận xét từ AI về tình hình kinh doanh.
        </p>
      )}
    </div>
  );
}
