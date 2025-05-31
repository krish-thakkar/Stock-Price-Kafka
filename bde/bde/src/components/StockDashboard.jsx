import React, { useState, useEffect } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Line, Bar } from 'react-chartjs-2';
import './Dashboard.css';

// Register ChartJS components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const companies = [
  { symbol: 'AAPL', name: 'Apple Inc.' },
  { symbol: 'MSFT', name: 'Microsoft Corporation' },
  { symbol: 'GOOGL', name: 'Alphabet Inc.' },
  { symbol: 'AMZN', name: 'Amazon.com Inc.' },
  { symbol: 'META', name: 'Meta Platforms Inc.' },
  { symbol: 'NVDA', name: 'NVIDIA Corporation' },
  { symbol: 'TSLA', name: 'Tesla Inc.' },
  { symbol: 'JPM', name: 'JPMorgan Chase & Co.' },
];

const StockDashboard = () => {
  const [selectedCompany, setSelectedCompany] = useState(companies[0].symbol);
  const [data, setData] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const response = await fetch(`http://localhost:3001/api/data/${selectedCompany}`);
        if (!response.ok) {
          throw new Error(`Failed to fetch data for ${selectedCompany}`);
        }
        const result = await response.json();
        setData(result.data || []); // Access the "data" property of the JSON
      } catch (err) {
        setError(err.message);
        setData([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [selectedCompany]);

  // Separate data arrays for the graphs
  const labels = data.map(item => item.date);
  const stockPrices = data.map(item => item.price);
  const tradingVolumes = data.map(item => item.volume);
  const sentimentScores = data.map(item => item.overallSentiment);
  const predictions = data.map(item => item.prediction);
  console.log(stockPrices);

  // Chart configurations
  const commonOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
      },
    },
  };

  const priceChartData = {
    labels,
    datasets: [
      {
        label: 'Stock Price',
        data: stockPrices,
        borderColor: 'rgb(37, 99, 235)',
        backgroundColor: 'rgba(37, 99, 235, 0.1)',
        fill: true,
      },
    ],
  };

  const volumeChartData = {
    labels,
    datasets: [
      {
        label: 'Trading Volume',
        data: tradingVolumes,
        backgroundColor: 'rgba(59, 130, 246, 0.8)',
      },
    ],
  };

  const sentimentChartData = {
    labels,
    datasets: [
      {
        label: 'Sentiment Score',
        data: sentimentScores,
        borderColor: 'rgb(16, 185, 129)',
        backgroundColor: 'rgba(16, 185, 129, 0.1)',
        fill: true,
      },
    ],
  };

  const combinedChartData = {
    labels,
    datasets: [
      {
        label: 'Actual Price',
        data: stockPrices,
        borderColor: 'rgb(37, 99, 235)',
        backgroundColor: 'rgba(37, 99, 235, 0.1)',
        fill: false,
      },
      {
        label: 'Predicted Price',
        data: predictions,
        borderColor: 'rgb(220, 38, 38)',
        backgroundColor: 'rgba(220, 38, 38, 0.1)',
        fill: false,
      },
    ],
  };

  const selectedCompanyData = companies.find(c => c.symbol === selectedCompany);

  return (
    <div className="dashboard-container">
      <div className="dashboard-content">
        <div className="dashboard-header">
          <h1 className="dashboard-title">Stock Market Analysis Dashboard</h1>
          <h2 className="dashboard-subtitle">
            {selectedCompanyData?.name} ({selectedCompanyData?.symbol})
          </h2>
        </div>

        <div className="companies-grid">
          {companies.map(company => (
            <button
              key={company.symbol}
              onClick={() => setSelectedCompany(company.symbol)}
              className={`company-button ${selectedCompany === company.symbol ? 'active' : ''}`}
            >
              <div className="company-symbol">{company.symbol}</div>
              <div className="company-name">{company.name}</div>
            </button>
          ))}
        </div>

        {isLoading ? (
          <div className="loading">Loading data...</div>
        ) : error ? (
          <div className="error">{error}</div>
        ) : (
          <div className="charts-grid">
            <div className="chart-container">
              <h2 className="chart-title">Stock Price Trend</h2>
              <div className="chart-wrapper">
                <Line data={priceChartData} options={commonOptions} />
              </div>
            </div>

            <div className="chart-container">
              <h2 className="chart-title">Trading Volume</h2>
              <div className="chart-wrapper">
                <Bar data={volumeChartData} options={commonOptions} />
              </div>
            </div>

            <div className="chart-container">
              <h2 className="chart-title">News Sentiment Analysis</h2>
              <div className="chart-wrapper">
                <Line
                  data={sentimentChartData}
                  options={{
                    ...commonOptions,
                    scales: {
                      y: {
                        min: -1,
                        max: 1,
                      },
                    },
                  }}
                />
              </div>
            </div>

            <div className="chart-container">
              <h2 className="chart-title">Combined Analysis & Prediction</h2>
              <div className="chart-wrapper">
                <Line data={combinedChartData} options={commonOptions} />
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default StockDashboard;
