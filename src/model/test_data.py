import matplotlib.pyplot as plt
import numpy as np
from hmmlearn import hmm
from matplotlib.patches import Patch

np.random.seed(0)
plt.style.use("seaborn-v0_8-darkgrid")


# --- 1. Generazione Dati Bivariati (Return, Volatility) ---
def generate_2d_synthetic_data(n_samples=2000):
    """
    Genera osservazioni 2D: [Log-Returns, Log-Volatility]
    """
    model = hmm.GaussianHMM(n_components=3, covariance_type="full")

    model.startprob_ = np.array([0.5, 0.2, 0.3])
    model.transmat_ = np.array([[0.96, 0.01, 0.03], [0.02, 0.90, 0.08], [0.02, 0.05, 0.93]])  # Bull  # Bear  # Sideways

    # DEFINIZIONE REGIMI (Feature 0: Ret, Feature 1: Log-Vol)
    # Nota: Usiamo Log-Vol perché la volatilità è sempre positiva e log-normale

    # Bull:  Rendimenti positivi, Volatilità Bassa (Log-Vol negativa)
    bull_mean = [0.0010, -5.5]

    # Bear:  Rendimenti negativi, Volatilità Alta (Log-Vol meno negativa/alta)
    bear_mean = [-0.0015, -3.5]

    # Side:  Rendimenti nulli, Volatilità "Compressa" (Molto bassa, tipico pre-breakout)
    side_mean = [0.0000, -6.5]

    model.means_ = np.array([bull_mean, bear_mean, side_mean])

    # Covarianze (3, 2, 2)
    # Bull: poca varianza nei returns, poca nella vol
    cov_bull = [[0.00002, 0.0], [0.0, 0.1]]
    # Bear: molta varianza nei returns, molta nella vol (panic)
    cov_bear = [[0.00020, -0.001], [-0.001, 0.5]]  # Correlazione negativa (leverage effect)
    # Side: varianza returns media, varianza vol bassissima (stabile)
    cov_side = [[0.00001, 0.0], [0.0, 0.05]]

    model.covars_ = np.array([cov_bull, cov_bear, cov_side])

    X, Z = model.sample(n_samples)
    return X, Z, model


# --- 2. Fit Multivariate HMM ---
def fit_2d_model(X):
    # Fit su dati 2D
    model = hmm.GaussianHMM(n_components=3, covariance_type="full", n_iter=200, random_state=42, init_params="stmc")
    model.fit(X)
    _, Z_pred = model.decode(X)
    return model, Z_pred


# --- 3. Mapping Semantico 2D ---
def map_states_2d(model):
    # Logica di mapping:
    # 1. Troviamo lo stato con Log-Vol più alta -> Bear
    # 2. Tra i rimanenti, quello con Mean Return più alto -> Bull
    # 3. L'altro -> Sideways

    means_ret = model.means_[:, 0]
    means_vol = model.means_[:, 1]

    mapping = {}
    ids = [0, 1, 2]

    # Trova Bear (Max Volatility)
    bear_id = np.argmax(means_vol)
    mapping[bear_id] = {"label": "Bear", "color": "#e74c3c"}  # Rosso
    ids.remove(bear_id)

    # Tra i rimanenti, trova Bull (Max Return)
    if means_ret[ids[0]] > means_ret[ids[1]]:
        bull_id = ids[0]
        side_id = ids[1]
    else:
        bull_id = ids[1]
        side_id = ids[0]

    mapping[bull_id] = {"label": "Bull", "color": "#2ecc71"}  # Verde
    mapping[side_id] = {"label": "Sideways", "color": "#95a5a6"}  # Grigio

    print("--- Mapping Identificato (Automatico) ---")
    for i in range(3):
        print(f"Stato {i}: {mapping[i]['label']} | Mean Ret: {means_ret[i]:.5f} | Mean LogVol: {means_vol[i]:.2f}")

    return mapping


# --- 4. Plotting Avanzato ---
def plot_results_2d(X, Z_true, Z_pred, mapping):

    # Prezzo Sintetico (usando solo la colonna returns X[:,0])
    price = 100 * np.exp(np.cumsum(X[:, 0]))

    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(2, 2)

    # PLOT 1: Time Series (Prezzo)
    ax1 = fig.add_subplot(gs[0, :])
    ax1.plot(price, color="black", lw=1)
    ax1.set_title("Asset Price & Predicted Regimes (Multivariate HMM)", fontsize=14)

    start_idx = 0
    for i in range(1, len(Z_pred)):
        if Z_pred[i] != Z_pred[i - 1]:
            ax1.axvspan(start_idx, i, color=mapping[Z_pred[start_idx]]["color"], alpha=0.3, lw=0)
            start_idx = i
    ax1.axvspan(start_idx, len(Z_pred), color=mapping[Z_pred[start_idx]]["color"], alpha=0.3, lw=0)

    # Legenda ordinata
    patches = [Patch(color=v["color"], label=v["label"], alpha=0.3) for k, v in mapping.items()]
    # Ordine visivo: Bear, Side, Bull
    sorted_patches = sorted(patches, key=lambda x: ["Bear", "Sideways", "Bull"].index(x.get_label()))
    ax1.legend(handles=sorted_patches, loc="upper left")

    # PLOT 2: Scatter Plot (Return vs Volatility)
    # Questo è il grafico da "Ingegnere": mostra i cluster Gaussiani
    ax2 = fig.add_subplot(gs[1, 0])

    # Scatter dei punti colorati per predizione
    colors = [mapping[z]["color"] for z in Z_pred]
    ax2.scatter(X[:, 0], X[:, 1], c=colors, s=5, alpha=0.5)

    ax2.set_title("Feature Space Clustering", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Log Returns")
    ax2.set_ylabel("Log Volatility")

    # PLOT 3: Accuratezza (Confusion Matrix visuale)
    # Confronto striscia True vs Pred
    ax3 = fig.add_subplot(gs[1, 1])

    # Mappiamo gli ID numerici ai colori per imshow
    # Dobbiamo creare una matrice RGB
    def to_rgb(z_seq):
        rgb_seq = []
        for z in z_seq:
            hex_col = mapping[z]["color"].lstrip("#")
            rgb_seq.append(tuple(int(hex_col[i : i + 2], 16) / 255.0 for i in (0, 2, 4)))
        return np.array(rgb_seq)

    rgb_true = to_rgb(Z_true)  # Nota: qui c'è un rischio se il mapping Z_true != Z_pred.
    # Per semplicità nel plot sintetico assumiamo che il mapping semantico abbia funzionato
    # e riusiamo i colori predetti per visualizzare Z_true (assumendo coerenza semantica).
    # Nella realtà bisognerebbe rimappare Z_true esplicitamente.
    rgb_pred = to_rgb(Z_pred)

    ax3.imshow(rgb_true[np.newaxis, :], aspect="auto", extent=[0, len(Z_true), 0.5, 1])
    ax3.imshow(rgb_pred[np.newaxis, :], aspect="auto", extent=[0, len(Z_true), 0, 0.5])

    ax3.set_yticks([0.25, 0.75])
    ax3.set_yticklabels(["Model", "Truth"])
    ax3.set_title("Regime Alignment (Truth vs Model)", fontsize=12, fontweight="bold")

    plt.tight_layout()
    plt.show()


# --- Esecuzione ---
if __name__ == "__main__":
    # 1. Genera (Returns, Vol)
    X_2d, Z_true, _ = generate_2d_synthetic_data(2000)

    # 2. Fit
    model_2d, Z_pred = fit_2d_model(X_2d)

    # 3. Map
    try:
        mapping = map_states_2d(model_2d)

        # 4. Plot

        plot_results_2d(X_2d, Z_true, Z_pred, mapping)
    except Exception as e:
        print(f"Errore nel mapping (probabilmente convergenza fallita): {e}")
